import {
	appendFileSync,
	closeSync,
	mkdirSync,
	mkdtempSync,
	openSync,
	readFileSync,
	rmSync,
	statSync,
	utimesSync,
	writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Worker } from "node:worker_threads";
import lockfile from "proper-lockfile";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
	parseSessionEntries,
	type SessionHeader,
	type SessionInfoEntry,
	SessionManager,
	type SessionNameState,
} from "../src/core/session-manager.ts";

const tempDirs: string[] = [];
const timestamp = "2026-08-12T00:00:00.000Z";
const lockfilePath = createRequire(import.meta.url).resolve("proper-lockfile");

function messageEntry(id: string, parentId: string | null, text: string): Record<string, unknown> {
	return {
		type: "message",
		id,
		parentId,
		timestamp,
		message: { role: "user", content: [{ type: "text", text }], timestamp: 1 },
	};
}

function createSessionFile(entries: Array<Record<string, unknown>> = []): string {
	const dir = mkdtempSync(join(tmpdir(), "pi-session-name-cas-"));
	tempDirs.push(dir);
	const file = join(dir, "session.jsonl");
	const header = {
		type: "session",
		version: 3,
		id: "session-name-cas",
		timestamp,
		cwd: dir,
	};
	writeFileSync(file, `${[header, ...entries].map((entry) => JSON.stringify(entry)).join("\n")}\n`);
	return file;
}

function readSessionInfoEntries(file: string): SessionInfoEntry[] {
	return parseSessionEntries(readFileSync(file, "utf8")).filter(
		(entry): entry is SessionInfoEntry => entry.type === "session_info",
	);
}

function initialNameState(): SessionNameState {
	return { name: undefined, entryId: undefined };
}

afterEach(() => {
	for (const dir of tempDirs.splice(0)) {
		rmSync(dir, { recursive: true, force: true });
	}
});

describe("SessionManager session name transactions", () => {
	it("reloads under the shared lock before a legacy whole-file migration rewrite", () => {
		const file = createSessionFile();
		const legacyHeader = JSON.parse(readFileSync(file, "utf8").trim()) as Record<string, unknown>;
		legacyHeader.version = 2;
		writeFileSync(file, `${JSON.stringify(legacyHeader)}\n`);
		const humanEntry = {
			type: "session_info",
			id: "human-during-migration",
			parentId: null,
			timestamp,
			name: "Human during migration",
			origin: "human",
		};
		const originalLockSync = lockfile.lockSync.bind(lockfile);
		const lockSpy = vi.spyOn(lockfile, "lockSync").mockImplementationOnce(((target: string, options: object) => {
			// Simulate a cooperating writer committing after the migration manager's
			// initial stale read but before it owns the shared physical-file lock.
			appendFileSync(file, `${JSON.stringify(humanEntry)}\n`);
			return originalLockSync(target, options as any);
		}) as typeof lockfile.lockSync);
		try {
			const manager = SessionManager.open(file);
			expect(manager.getSessionName()).toBe("Human during migration");
			const entries = parseSessionEntries(readFileSync(file, "utf8"));
			expect((entries[0] as unknown as Record<string, unknown>).version).toBe(3);
			expect(readSessionInfoEntries(file)).toEqual([humanEntry]);
		} finally {
			lockSpy.mockRestore();
		}
	});

	it("publishes legacy migration rewrites by atomic replacement", () => {
		const file = createSessionFile([messageEntry("legacy-message", null, "legacy")]);
		const legacyEntries = parseSessionEntries(readFileSync(file, "utf8"));
		(legacyEntries[0] as SessionHeader).version = 2;
		writeFileSync(file, `${legacyEntries.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
		const before = statSync(file);

		SessionManager.open(file);

		const after = statSync(file);
		expect(after.ino).not.toBe(before.ino);
		expect((parseSessionEntries(readFileSync(file, "utf8"))[0] as SessionHeader).version).toBe(3);
	});

	it("conditionally writes only when the exact physical name revision matches", () => {
		const file = createSessionFile();
		const first = SessionManager.open(file);
		const stale = SessionManager.open(file);

		const result = first.appendSessionInfoIfCurrent("Generated title", first.getSessionNameState(), {
			origin: "automatic",
		});
		expect(result).toMatchObject({ written: true, entryId: expect.any(String) });

		const refused = stale.appendSessionInfoIfCurrent("Stale generated title", initialNameState(), {
			origin: "automatic",
		});
		const writtenEntry = readSessionInfoEntries(file)[0];
		expect(refused).toEqual({
			written: false,
			currentState: { name: "Generated title", entryId: writtenEntry.id },
		});
		expect(stale.getSessionNameState()).toEqual({ name: "Generated title", entryId: writtenEntry.id });
		expect(writtenEntry).toMatchObject({ name: "Generated title", origin: "automatic" });
	});

	it("leaves a human name latest when automatic naming acquires the lock first", () => {
		const file = createSessionFile();
		const automaticManager = SessionManager.open(file);
		const humanManager = SessionManager.open(file);

		expect(
			automaticManager.appendSessionInfoIfCurrent("Automatic", automaticManager.getSessionNameState(), {
				origin: "automatic",
			}).written,
		).toBe(true);
		humanManager.appendSessionInfo("Human", { origin: "human" });

		expect(readSessionInfoEntries(file).map(({ name, origin }) => ({ name, origin }))).toEqual([
			{ name: "Automatic", origin: "automatic" },
			{ name: "Human", origin: "human" },
		]);
		expect(SessionManager.open(file).getSessionName()).toBe("Human");
	});

	it("refuses automatic naming when human naming acquires the lock first", () => {
		const file = createSessionFile();
		const humanManager = SessionManager.open(file);
		const automaticManager = SessionManager.open(file);
		const expected = automaticManager.getSessionNameState();

		humanManager.appendSessionInfo("Human", { origin: "human" });
		const automatic = automaticManager.appendSessionInfoIfCurrent("Automatic", expected, {
			origin: "automatic",
		});

		expect(automatic).toMatchObject({ written: false, currentState: { name: "Human" } });
		expect(readSessionInfoEntries(file).map((entry) => entry.name)).toEqual(["Human"]);
	});

	it("treats explicit clears and same-value writes as new revisions", () => {
		const clearFile = createSessionFile();
		const staleBeforeClear = SessionManager.open(clearFile);
		const clearWriter = SessionManager.open(clearFile);
		const expectedMissing = staleBeforeClear.getSessionNameState();
		const clearId = clearWriter.appendSessionInfo("", { origin: "human" });

		expect(staleBeforeClear.appendSessionInfoIfCurrent("Automatic", expectedMissing)).toEqual({
			written: false,
			currentState: { name: undefined, entryId: clearId },
		});

		const sameFile = createSessionFile();
		const firstWriter = SessionManager.open(sameFile);
		firstWriter.appendSessionInfo("Same", { origin: "human" });
		const staleBeforeSameValue = SessionManager.open(sameFile);
		const expectedSame = staleBeforeSameValue.getSessionNameState();
		const sameValueId = firstWriter.appendSessionInfo("Same", { origin: "human" });

		expect(staleBeforeSameValue.appendSessionInfoIfCurrent("Automatic", expectedSame)).toEqual({
			written: false,
			currentState: { name: "Same", entryId: sameValueId },
		});
	});

	it("waits for a cooperating writer and revalidates physical state under the lock", async () => {
		const file = createSessionFile();
		const automaticManager = SessionManager.open(file);
		const worker = new Worker(
			`
				const { appendFileSync } = require("node:fs");
				const { parentPort, workerData } = require("node:worker_threads");
				const lockfile = require(workerData.lockfilePath);
				const release = lockfile.lockSync(workerData.file, { stale: 600000, retries: 0 });
				parentPort.postMessage("locked");
				setTimeout(() => {
					appendFileSync(workerData.file, JSON.stringify({
						type: "session_info",
						id: "worker-human-name",
						parentId: null,
						timestamp: workerData.timestamp,
						name: "Human from worker",
						origin: "human",
					}) + "\\n");
					release();
				}, 100);
			`,
			{ eval: true, workerData: { file, timestamp, lockfilePath } },
		);
		await new Promise<void>((resolve, reject) => {
			worker.once("message", () => resolve());
			worker.once("error", reject);
		});

		try {
			expect(
				automaticManager.appendSessionInfoIfCurrent("Automatic", initialNameState(), {
					origin: "automatic",
				}),
			).toEqual({
				written: false,
				currentState: { name: "Human from worker", entryId: "worker-human-name" },
			});
		} finally {
			await worker.terminate();
		}
	});

	it("keeps new session_info entries out of the conversation parent chain", () => {
		const file = createSessionFile([messageEntry("message-one", null, "one")]);
		const manager = SessionManager.open(file);
		const nameId = manager.appendSessionInfo("Metadata", { origin: "human" });

		expect(manager.getLeafId()).toBe("message-one");
		expect(manager.getBranch().map((entry) => entry.id)).toEqual(["message-one"]);
		const secondId = manager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "two" }],
			timestamp: 2,
		});
		expect(manager.getEntry(secondId)?.parentId).toBe("message-one");
		expect(manager.getEntry(nameId)?.parentId).toBe("message-one");

		const reopened = SessionManager.open(file);
		expect(reopened.getLeafId()).toBe(secondId);
		expect(reopened.buildSessionContext().messages.map((message) => message.role)).toEqual(["user", "user"]);
	});

	it("keeps a stale cooperating message append on the active conversation after naming", () => {
		const file = createSessionFile([messageEntry("message-one", null, "one")]);
		const namingManager = SessionManager.open(file);
		const messageManager = SessionManager.open(file);

		namingManager.appendSessionInfo("Metadata", { origin: "human" });
		const secondId = messageManager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "two" }],
			timestamp: 2,
		});

		expect(messageManager.getEntry(secondId)?.parentId).toBe("message-one");
		const reopened = SessionManager.open(file);
		expect(reopened.getLeafId()).toBe(secondId);
		expect(reopened.buildSessionContext().messages).toHaveLength(2);
	});

	it("refreshes only physical name metadata without replacing conversation state", () => {
		const file = createSessionFile([
			messageEntry("message-one", null, "one"),
			messageEntry("message-two", "message-one", "two"),
		]);
		const namingManager = SessionManager.open(file);
		const physicalWriter = SessionManager.open(file);
		const expected = namingManager.getSessionNameState();
		const originalConversationEntries = namingManager.getEntries().filter((entry) => entry.type === "message");

		physicalWriter.appendMessage({
			role: "user",
			content: [{ type: "text", text: "external" }],
			timestamp: 3,
		});
		const humanNameId = physicalWriter.appendSessionInfo("Human", { origin: "human" });
		const result = namingManager.appendSessionInfoIfCurrent("Automatic", expected, { origin: "automatic" });

		expect(result).toEqual({
			written: false,
			currentState: { name: "Human", entryId: humanNameId },
		});
		expect(namingManager.getLeafId()).toBe("message-two");
		expect(namingManager.getEntry("message-two")).toBe(originalConversationEntries[1]);
		expect(namingManager.getEntries().filter((entry) => entry.type === "message")).toEqual(
			originalConversationEntries,
		);
		expect(namingManager.getSessionNameState()).toEqual({ name: "Human", entryId: humanNameId });
	});

	it("loads legacy messages whose parent is a session_info entry", () => {
		const file = createSessionFile([
			messageEntry("message-one", null, "one"),
			{ type: "session_info", id: "legacy-name", parentId: "message-one", timestamp, name: "Legacy" },
			messageEntry("message-two", "legacy-name", "two"),
		]);
		const manager = SessionManager.open(file);

		expect(manager.getLeafId()).toBe("message-two");
		expect(manager.buildSessionContext().messages).toHaveLength(2);
	});

	it("rejects session_info as a new branch or branch-summary target", () => {
		const file = createSessionFile([messageEntry("message-one", null, "one")]);
		const manager = SessionManager.open(file);
		const nameId = manager.appendSessionInfo("Metadata", { origin: "human" });

		expect(() => manager.branch(nameId)).toThrow(/metadata cannot be selected/);
		expect(() => manager.branchWithSummary(nameId, "summary")).toThrow(/metadata cannot be selected/);
		expect(manager.getLeafId()).toBe("message-one");
	});

	it("preserves selected branches and reset leaves on success and refused CAS", () => {
		const file = createSessionFile([
			messageEntry("message-one", null, "one"),
			messageEntry("message-two", "message-one", "two"),
		]);
		const selected = SessionManager.open(file);
		selected.branch("message-one");
		const expected = selected.getSessionNameState();
		SessionManager.open(file).appendSessionInfo("Human", { origin: "human" });

		expect(selected.appendSessionInfoIfCurrent("Automatic", expected)).toMatchObject({ written: false });
		expect(selected.getLeafId()).toBe("message-one");
		selected.appendSessionInfo("Human override", { origin: "human" });
		expect(selected.getLeafId()).toBe("message-one");

		const reset = SessionManager.open(file);
		reset.resetLeaf();
		const resetExpected = reset.getSessionNameState();
		SessionManager.open(file).appendSessionInfo("Another human", { origin: "human" });
		expect(reset.appendSessionInfoIfCurrent("Automatic", resetExpected)).toMatchObject({ written: false });
		expect(reset.getLeafId()).toBeNull();
		reset.appendSessionInfo("Reset override", { origin: "human" });
		expect(reset.getLeafId()).toBeNull();
	});

	it("reads legacy missing origin as human/unknown and preserves Unicode", () => {
		const file = createSessionFile([
			{ type: "session_info", id: "legacy-name", parentId: null, timestamp, name: "Legacy" },
		]);
		const manager = SessionManager.open(file);
		expect(manager.getLeafId()).toBeNull();
		const result = manager.appendSessionInfoIfCurrent("  東京 🐦\r\n計画  ", manager.getSessionNameState(), {
			origin: "automatic",
		});

		expect(result.written).toBe(true);
		const entries = readSessionInfoEntries(file);
		expect(entries[0].origin).toBeUndefined();
		expect(entries[1]).toMatchObject({ name: "東京 🐦 計画", origin: "automatic" });
	});

	it("fails safely on live locks but recovers a genuinely stale lock", () => {
		const liveFile = createSessionFile();
		const liveManager = SessionManager.open(liveFile);
		const before = readFileSync(liveFile, "utf8");
		mkdirSync(`${liveFile}.lock`);
		expect(() => liveManager.appendSessionInfo("Blocked", { origin: "human" })).toThrow();
		expect(readFileSync(liveFile, "utf8")).toBe(before);

		const staleFile = createSessionFile();
		const staleManager = SessionManager.open(staleFile);
		mkdirSync(`${staleFile}.lock`);
		const oldTime = new Date(Date.now() - 25 * 60 * 60 * 1000);
		utimesSync(`${staleFile}.lock`, oldTime, oldTime);
		expect(staleManager.appendSessionInfo("Recovered", { origin: "human" })).toEqual(expect.any(String));
		expect(SessionManager.open(staleFile).getSessionName()).toBe("Recovered");
	});

	it("strictly scans sessions larger than 64 MiB without a rename ceiling", () => {
		const file = createSessionFile();
		const manager = SessionManager.open(file);
		const fd = openSync(file, "a");
		const padding = "x".repeat(4_000);
		try {
			for (let batchStart = 0; batchStart < 17_000; batchStart += 500) {
				const lines: string[] = [];
				for (let index = batchStart; index < Math.min(batchStart + 500, 17_000); index++) {
					lines.push(
						JSON.stringify({
							type: "custom",
							id: index.toString(16).padStart(8, "0"),
							parentId: null,
							timestamp,
							customType: "large-session-fixture",
							data: padding,
						}),
					);
				}
				writeFileSync(fd, `${lines.join("\n")}\n`);
			}
		} finally {
			closeSync(fd);
		}

		const sizeBeforeName = statSync(file).size;
		expect(sizeBeforeName).toBeGreaterThan(64 * 1024 * 1024);
		expect(manager.appendSessionInfo("Large session", { origin: "human" })).toEqual(expect.any(String));
		expect(statSync(file).size).toBeGreaterThan(sizeBeforeName);
		expect(manager.getSessionName()).toBe("Large session");
	});

	it.each([
		["malformed", '{"type":\n'],
		["unterminated", JSON.stringify({ type: "session_info", id: "tail", parentId: null, timestamp, name: "Tail" })],
	])("fails closed on a %s JSONL tail", (_kind, tail) => {
		const file = createSessionFile();
		appendFileSync(file, tail);
		const manager = SessionManager.open(file);
		const before = readFileSync(file, "utf8");

		expect(() => manager.appendSessionInfo("Must not append", { origin: "human" })).toThrow(/JSONL/);
		expect(readFileSync(file, "utf8")).toBe(before);
	});

	it("keeps exact revision CAS and non-structural metadata in memory", () => {
		const manager = SessionManager.inMemory("/synthetic");
		const initial = manager.getSessionNameState();
		const automatic = manager.appendSessionInfoIfCurrent("Automatic", initial, { origin: "automatic" });
		expect(automatic.written).toBe(true);
		const automaticState = manager.getSessionNameState();
		const humanId = manager.appendSessionInfo("Automatic");
		expect(manager.getEntry(humanId)).toMatchObject({ origin: "human" });
		expect(manager.appendSessionInfoIfCurrent("Second", automaticState)).toEqual({
			written: false,
			currentState: { name: "Automatic", entryId: humanId },
		});
		expect(manager.getLeafId()).toBeNull();
	});
});
