import type * as FsModule from "node:fs";
import {
	appendFileSync,
	closeSync,
	existsSync,
	linkSync,
	lstatSync,
	mkdirSync,
	mkdtempSync,
	openSync,
	readdirSync,
	readFileSync,
	rmSync,
	statSync,
	symlinkSync,
	unlinkSync,
	utimesSync,
	writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
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

const scanProbe = vi.hoisted(() => ({
	lockHeld: false,
	readsWhileLocked: 0,
	materializeUnlinkFailures: 0,
	materializeConcurrentAppendLine: undefined as string | undefined,
	directoryFsyncErrorCode: undefined as string | undefined,
	revisionRaceFile: undefined as string | undefined,
	revisionRaceLine: undefined as string | undefined,
	revisionRaceReads: 0,
}));
vi.mock("fs", async (importOriginal) => {
	const actual = await importOriginal<typeof FsModule>();
	return {
		...actual,
		readSync(
			fd: number,
			buffer: NodeJS.ArrayBufferView,
			offset: number,
			length: number,
			position: number | null,
		): number {
			if (scanProbe.lockHeld) scanProbe.readsWhileLocked++;
			if (scanProbe.revisionRaceFile && scanProbe.revisionRaceLine && scanProbe.revisionRaceReads > 0) {
				scanProbe.revisionRaceReads--;
				if (scanProbe.revisionRaceReads === 0) {
					actual.appendFileSync(scanProbe.revisionRaceFile, scanProbe.revisionRaceLine);
					scanProbe.revisionRaceFile = undefined;
					scanProbe.revisionRaceLine = undefined;
				}
			}
			return actual.readSync(fd, buffer, offset, length, position);
		},
		unlinkSync(path: Parameters<typeof actual.unlinkSync>[0]): void {
			const pathText = String(path);
			if (pathText.includes(".materialize-") && scanProbe.materializeUnlinkFailures > 0) {
				scanProbe.materializeUnlinkFailures--;
				if (scanProbe.materializeConcurrentAppendLine) {
					actual.appendFileSync(
						pathText.slice(0, pathText.indexOf(".materialize-")),
						scanProbe.materializeConcurrentAppendLine,
					);
					scanProbe.materializeConcurrentAppendLine = undefined;
				}
				throw Object.assign(new Error("injected materialize unlink failure"), { code: "EIO" });
			}
			actual.unlinkSync(path);
		},
		fsyncSync(fd: number): void {
			if (scanProbe.directoryFsyncErrorCode && actual.fstatSync(fd).isDirectory()) {
				throw Object.assign(new Error("injected directory fsync failure"), {
					code: scanProbe.directoryFsyncErrorCode,
				});
			}
			actual.fsyncSync(fd);
		},
	};
});

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
	scanProbe.lockHeld = false;
	scanProbe.readsWhileLocked = 0;
	scanProbe.materializeUnlinkFailures = 0;
	scanProbe.materializeConcurrentAppendLine = undefined;
	scanProbe.directoryFsyncErrorCode = undefined;
	scanProbe.revisionRaceFile = undefined;
	scanProbe.revisionRaceLine = undefined;
	scanProbe.revisionRaceReads = 0;
	for (const dir of tempDirs.splice(0)) {
		rmSync(dir, { recursive: true, force: true });
	}
});

describe("SessionManager session name transactions", () => {
	it("retries a revision-raced migration without losing a concurrent append", () => {
		const file = createSessionFile();
		const legacyHeader = JSON.parse(readFileSync(file, "utf8").trim()) as Record<string, unknown>;
		legacyHeader.version = 2;
		writeFileSync(file, `${JSON.stringify(legacyHeader)}\n`);
		const concurrentEntry = messageEntry("concurrent-message", null, "concurrent append");
		const originalLockSync = lockfile.lockSync.bind(lockfile);
		const lockSpy = vi.spyOn(lockfile, "lockSync").mockImplementationOnce(((
			target: string,
			options: Parameters<typeof lockfile.lockSync>[1],
		) => {
			// Simulate a cooperating writer committing after the outside-lock scan
			// but before the migration's revision-validated commit.
			const releaseConcurrentWriter = originalLockSync(target, options);
			try {
				appendFileSync(file, `${JSON.stringify(concurrentEntry)}\n`);
			} finally {
				releaseConcurrentWriter();
			}
			return originalLockSync(target, options);
		}) as typeof lockfile.lockSync);
		try {
			const manager = SessionManager.open(file);
			const entries = parseSessionEntries(readFileSync(file, "utf8"));
			expect((entries[0] as unknown as Record<string, unknown>).version).toBe(3);
			expect(manager.getEntry("concurrent-message")).toMatchObject(concurrentEntry);
			expect(entries).toContainEqual(concurrentEntry);
			expect(lockSpy).toHaveBeenCalledTimes(2);
		} finally {
			lockSpy.mockRestore();
		}
	});

	it("performs transcript and name scans before acquiring the commit lock", () => {
		const file = createSessionFile([messageEntry("legacy-message", null, "legacy")]);
		const legacyEntries = parseSessionEntries(readFileSync(file, "utf8"));
		(legacyEntries[0] as SessionHeader).version = 2;
		writeFileSync(file, `${legacyEntries.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
		const originalLockSync = lockfile.lockSync.bind(lockfile);
		scanProbe.lockHeld = false;
		scanProbe.readsWhileLocked = 0;
		const lockSpy = vi.spyOn(lockfile, "lockSync").mockImplementation(((
			target: string,
			options: Parameters<typeof lockfile.lockSync>[1],
		) => {
			const release = originalLockSync(target, options);
			scanProbe.lockHeld = true;
			return () => {
				scanProbe.lockHeld = false;
				release();
			};
		}) as typeof lockfile.lockSync);
		try {
			const manager = SessionManager.open(file);
			manager.appendSessionInfo("Outside-lock scan", { origin: "human" });
			expect(lockSpy).toHaveBeenCalledTimes(2);
			expect(scanProbe.readsWhileLocked).toBe(0);
		} finally {
			scanProbe.lockHeld = false;
			lockSpy.mockRestore();
		}
	});

	it("materializes a new session without changing its identity or losing buffered entries", () => {
		const dir = mkdtempSync(join(tmpdir(), "pi-session-materialize-"));
		tempDirs.push(dir);
		const manager = SessionManager.create(dir, dir, { id: "materialized-session" });
		const file = manager.getSessionFile();
		expect(file).toBeDefined();
		expect(existsSync(file!)).toBe(false);
		manager.appendCustomEntry("buffered", { retained: true });

		manager.materialize();

		expect(existsSync(file!)).toBe(true);
		expect(SessionManager.open(file!).getSessionId()).toBe("materialized-session");
		expect(
			SessionManager.open(file!)
				.getEntries()
				.some((entry) => entry.type === "custom"),
		).toBe(true);
		expect(() => manager.materialize()).not.toThrow();
	});

	it("reconciles a crash after materialize link publication", () => {
		const dir = mkdtempSync(join(tmpdir(), "pi-session-materialize-crash-"));
		tempDirs.push(dir);
		const manager = SessionManager.create(dir, dir, { id: "materialize-crash-session" });
		manager.appendCustomEntry("buffered", { retained: true });
		const file = manager.getSessionFile()!;
		const temporary = `${file}.materialize-crashed`;
		const entries = [manager.getHeader(), ...manager.getEntries()];
		writeFileSync(temporary, `${entries.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
		linkSync(temporary, file);

		const reopened = SessionManager.open(file);

		expect(existsSync(file)).toBe(true);
		expect(existsSync(temporary)).toBe(false);
		expect(reopened.getEntry(manager.getEntries()[0].id)).toBeDefined();
	});

	it("treats post-link cleanup failure as committed and removes the safe alias", () => {
		const dir = mkdtempSync(join(tmpdir(), "pi-session-materialize-cleanup-"));
		tempDirs.push(dir);
		const manager = SessionManager.create(dir, dir, { id: "materialize-cleanup-session" });
		manager.appendCustomEntry("buffered");
		const file = manager.getSessionFile()!;
		scanProbe.materializeUnlinkFailures = 1;

		expect(() => manager.materialize()).not.toThrow();

		expect(existsSync(file)).toBe(true);
		expect(readdirSync(dir).filter((name) => name.includes(".materialize-"))).toEqual([]);
		expect(() => manager.materialize()).not.toThrow();
	});

	it("keeps a concurrent complete append during post-publication materialize recovery", () => {
		const dir = mkdtempSync(join(tmpdir(), "pi-session-materialize-concurrent-"));
		tempDirs.push(dir);
		const manager = SessionManager.create(dir, dir, { id: "materialize-concurrent-session" });
		const managerEntryId = manager.appendCustomEntry("buffered");
		const file = manager.getSessionFile()!;
		const concurrentEntry = {
			type: "custom",
			id: "concurrent-materialize-entry",
			parentId: managerEntryId,
			timestamp,
			customType: "concurrent-materialize",
		};
		scanProbe.materializeUnlinkFailures = 1;
		scanProbe.materializeConcurrentAppendLine = `${JSON.stringify(concurrentEntry)}\n`;

		expect(() => manager.materialize()).not.toThrow();

		const entries = parseSessionEntries(readFileSync(file, "utf8"));
		expect(entries).toContainEqual(manager.getEntry(managerEntryId));
		expect(entries).toContainEqual(concurrentEntry);
		expect(readdirSync(dir).filter((name) => name.includes(".materialize-"))).toEqual([]);
		expect(() => manager.materialize()).not.toThrow();
		expect(parseSessionEntries(readFileSync(file, "utf8"))).toEqual(entries);
	});

	it.each(["EINVAL", "ENOTSUP", "EOPNOTSUPP", "ENOSYS", "EBADF"])(
		"treats unsupported directory fsync error %s as best-effort",
		(code) => {
			const dir = mkdtempSync(join(tmpdir(), "pi-session-materialize-fsync-"));
			tempDirs.push(dir);
			const manager = SessionManager.create(dir, dir, { id: `materialize-fsync-${code}` });
			manager.appendCustomEntry("buffered");
			scanProbe.directoryFsyncErrorCode = code;

			expect(() => manager.materialize()).not.toThrow();
			expect(existsSync(manager.getSessionFile()!)).toBe(true);
		},
	);

	it("keeps migrated manager state after a post-replacement directory fsync failure", () => {
		const file = createSessionFile([messageEntry("legacy-message", null, "legacy")]);
		const entries = parseSessionEntries(readFileSync(file, "utf8"));
		(entries[0] as SessionHeader).version = 2;
		writeFileSync(file, `${entries.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
		scanProbe.directoryFsyncErrorCode = "EIO";

		const manager = SessionManager.open(file);

		expect(manager.getHeader()?.version).toBe(3);
		expect((parseSessionEntries(readFileSync(file, "utf8"))[0] as SessionHeader).version).toBe(3);
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

	it("migrates a symlinked legacy session through one canonical physical identity", () => {
		const target = createSessionFile([messageEntry("legacy-message", null, "legacy")]);
		const legacyEntries = parseSessionEntries(readFileSync(target, "utf8"));
		(legacyEntries[0] as SessionHeader).version = 2;
		writeFileSync(target, `${legacyEntries.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
		const alias = join(dirname(target), "session-alias.jsonl");
		symlinkSync(target, alias);

		const manager = SessionManager.open(alias);

		expect(lstatSync(alias).isSymbolicLink()).toBe(true);
		expect(manager.getSessionFile()).toBe(target);
		expect((parseSessionEntries(readFileSync(target, "utf8"))[0] as SessionHeader).version).toBe(3);
		manager.appendSessionInfo("Canonical name", { origin: "human" });
		expect(SessionManager.open(alias).getSessionName()).toBe("Canonical name");
		expect(SessionManager.open(target).getSessionName()).toBe("Canonical name");
		expect(SessionManager.open(alias).getSessionId()).toBe(SessionManager.open(target).getSessionId());
	});

	it("binds one resolved target when a session symlink is retargeted during open", () => {
		const firstTarget = createSessionFile();
		const firstEntries = parseSessionEntries(readFileSync(firstTarget, "utf8"));
		(firstEntries[0] as SessionHeader).version = 2;
		writeFileSync(firstTarget, `${firstEntries.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
		const secondTarget = join(dirname(firstTarget), "second-session.jsonl");
		const secondEntries = structuredClone(firstEntries);
		(secondEntries[0] as SessionHeader).version = 3;
		(secondEntries[0] as SessionHeader).id = "second-session-id";
		writeFileSync(secondTarget, `${secondEntries.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
		const alias = join(dirname(firstTarget), "retargeted-alias.jsonl");
		symlinkSync(firstTarget, alias);
		const originalLockSync = lockfile.lockSync.bind(lockfile);
		const lockSpy = vi.spyOn(lockfile, "lockSync").mockImplementationOnce(((
			target: string,
			options: Parameters<typeof lockfile.lockSync>[1],
		) => {
			unlinkSync(alias);
			symlinkSync(secondTarget, alias);
			return originalLockSync(target, options);
		}) as typeof lockfile.lockSync);
		try {
			const manager = SessionManager.open(alias);
			expect(manager.getSessionFile()).toBe(firstTarget);
			expect(manager.getSessionId()).toBe("session-name-cas");
			expect(SessionManager.open(alias).getSessionId()).toBe("second-session-id");
		} finally {
			lockSpy.mockRestore();
		}
	});

	it("rejects dangling symlinks and stable hardlinked sessions without changing them", () => {
		const file = createSessionFile();
		const dangling = join(dirname(file), "dangling-session.jsonl");
		symlinkSync(join(dirname(file), "missing-target.jsonl"), dangling);
		expect(() => SessionManager.open(dangling)).toThrow(/does not resolve to an existing file/);

		const hardLink = join(dirname(file), "hard-linked-session.jsonl");
		linkSync(file, hardLink);
		const before = readFileSync(file, "utf8");
		expect(() => SessionManager.open(file)).toThrow(/multiple hard links.*cannot be safely opened/);
		expect(() => SessionManager.open(hardLink)).toThrow(/multiple hard links.*cannot be safely opened/);
		expect(readFileSync(file, "utf8")).toBe(before);
		expect(readFileSync(hardLink, "utf8")).toBe(before);
	});

	it("fails closed before replacing a multiply linked legacy session", () => {
		const file = createSessionFile([messageEntry("legacy-message", null, "legacy")]);
		const entries = parseSessionEntries(readFileSync(file, "utf8"));
		(entries[0] as SessionHeader).version = 2;
		writeFileSync(file, `${entries.map((entry) => JSON.stringify(entry)).join("\n")}\n`);
		const hardLink = join(dirname(file), "hard-linked-legacy-session.jsonl");
		linkSync(file, hardLink);
		const before = readFileSync(file, "utf8");

		expect(() => SessionManager.open(file)).toThrow(/multiple hard links.*cannot be safely opened/);
		expect(() => SessionManager.open(hardLink)).toThrow(/multiple hard links.*cannot be safely opened/);
		expect(readFileSync(file, "utf8")).toBe(before);
		expect(readFileSync(hardLink, "utf8")).toBe(before);
	});

	it("forks from one stable canonical symlink target and records its physical parent", () => {
		const source = createSessionFile([messageEntry("source-message", null, "source")]);
		const alias = join(dirname(source), "source-alias.jsonl");
		symlinkSync(source, alias);
		const targetDir = mkdtempSync(join(tmpdir(), "pi-session-fork-target-"));
		tempDirs.push(targetDir);

		const fork = SessionManager.forkFrom(alias, targetDir, targetDir, { id: "canonical-fork" });

		expect(fork.getHeader()?.parentSession).toBe(source);
		expect(fork.getEntry("source-message")).toBeDefined();
	});

	it("migrates a stable v1 fork snapshot without changing the source", () => {
		const source = createSessionFile();
		const sourceHeader = parseSessionEntries(readFileSync(source, "utf8"))[0] as SessionHeader;
		delete sourceHeader.version;
		const v1Messages = [
			{
				type: "message",
				timestamp,
				message: { role: "user", content: [{ type: "text", text: "first" }], timestamp: 1 },
			},
			{
				type: "message",
				timestamp,
				message: { role: "user", content: [{ type: "text", text: "second" }], timestamp: 2 },
			},
		];
		writeFileSync(source, `${[sourceHeader, ...v1Messages].map((entry) => JSON.stringify(entry)).join("\n")}\n`);
		const sourceBefore = readFileSync(source);
		const targetDir = mkdtempSync(join(tmpdir(), "pi-session-fork-v1-"));
		tempDirs.push(targetDir);

		const fork = SessionManager.forkFrom(source, targetDir, targetDir, { id: "migrated-v1-fork" });

		const entries = fork.getEntries();
		expect(fork.getHeader()?.version).toBe(3);
		expect(entries).toHaveLength(2);
		expect(new Set(entries.map((entry) => entry.id)).size).toBe(2);
		expect(entries[0].parentId).toBeNull();
		expect(entries[1].parentId).toBe(entries[0].id);
		expect(fork.buildSessionContext().messages).toMatchObject([
			{ content: [{ type: "text", text: "first" }] },
			{ content: [{ type: "text", text: "second" }] },
		]);
		expect(readFileSync(source)).toEqual(sourceBefore);
	});

	it("rejects dangling and hardlinked fork sources without publishing a target", () => {
		const source = createSessionFile([messageEntry("source-message", null, "source")]);
		const dangling = join(dirname(source), "dangling-fork-source.jsonl");
		symlinkSync(join(dirname(source), "missing-fork-source.jsonl"), dangling);
		const hardLink = join(dirname(source), "hardlinked-fork-source.jsonl");
		linkSync(source, hardLink);
		const before = readFileSync(source, "utf8");
		const targetDir = mkdtempSync(join(tmpdir(), "pi-session-fork-reject-"));
		tempDirs.push(targetDir);

		expect(() => SessionManager.forkFrom(dangling, targetDir, targetDir)).toThrow(
			/does not resolve to an existing file/,
		);
		expect(() => SessionManager.forkFrom(source, targetDir, targetDir)).toThrow(
			/multiple hard links.*cannot be safely opened/,
		);
		expect(readFileSync(source, "utf8")).toBe(before);
		expect(readFileSync(hardLink, "utf8")).toBe(before);
		expect(readdirSync(targetDir).filter((name) => name.endsWith(".jsonl"))).toEqual([]);
	});

	it("retries a fork source revision race without producing a torn fork", () => {
		const source = createSessionFile([messageEntry("source-message", null, "source")]);
		const concurrentEntry = messageEntry("fork-concurrent-message", "source-message", "concurrent");
		const targetDir = mkdtempSync(join(tmpdir(), "pi-session-fork-race-"));
		tempDirs.push(targetDir);
		scanProbe.revisionRaceFile = source;
		scanProbe.revisionRaceLine = `${JSON.stringify(concurrentEntry)}\n`;
		scanProbe.revisionRaceReads = 1;

		const fork = SessionManager.forkFrom(source, targetDir, targetDir, { id: "revision-race-fork" });

		expect(fork.getEntry("source-message")).toBeDefined();
		expect(fork.getEntry("fork-concurrent-message")).toMatchObject(concurrentEntry);
		expect(parseSessionEntries(readFileSync(fork.getSessionFile()!, "utf8"))).toContainEqual(concurrentEntry);
	});

	it("pins a symlinked session directory before materializing a new session", () => {
		const root = mkdtempSync(join(tmpdir(), "pi-session-dir-alias-"));
		tempDirs.push(root);
		const firstDir = join(root, "first");
		const secondDir = join(root, "second");
		mkdirSync(firstDir);
		mkdirSync(secondDir);
		const aliasDir = join(root, "current");
		symlinkSync(firstDir, aliasDir, "dir");
		const manager = SessionManager.create(root, aliasDir, { id: "pinned-directory-session" });
		const file = manager.getSessionFile()!;
		expect(dirname(file)).toBe(firstDir);
		unlinkSync(aliasDir);
		symlinkSync(secondDir, aliasDir, "dir");

		manager.materialize();

		expect(existsSync(join(firstDir, basename(file)))).toBe(true);
		expect(existsSync(join(secondDir, basename(file)))).toBe(false);
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
		const beforeState = liveManager.getSessionNameState();
		mkdirSync(`${liveFile}.lock`);
		expect(() => liveManager.appendSessionInfo("Blocked", { origin: "human" })).toThrow();
		expect(readFileSync(liveFile, "utf8")).toBe(before);
		expect(liveManager.getSessionNameState()).toEqual(beforeState);

		const staleFile = createSessionFile();
		const staleManager = SessionManager.open(staleFile);
		mkdirSync(`${staleFile}.lock`);
		const oldTime = new Date(Date.now() - 6 * 60 * 1000);
		utimesSync(`${staleFile}.lock`, oldTime, oldTime);
		expect(staleManager.appendSessionInfo("Recovered", { origin: "human" })).toEqual(expect.any(String));
		expect(SessionManager.open(staleFile).getSessionName()).toBe("Recovered");
	});

	it("rolls back a failed session switch and ordinary append", () => {
		const firstFile = createSessionFile([messageEntry("first-message", null, "first")]);
		const manager = SessionManager.open(firstFile);
		const previousFile = manager.getSessionFile();
		const previousId = manager.getSessionId();
		const previousLeaf = manager.getLeafId();
		const invalidFile = join(dirname(firstFile), "invalid-session.jsonl");
		writeFileSync(invalidFile, "not-json\n");

		expect(() => manager.setSessionFile(invalidFile)).toThrow(/not a valid pi session/);
		expect(manager.getSessionFile()).toBe(previousFile);
		expect(manager.getSessionId()).toBe(previousId);
		expect(manager.getLeafId()).toBe(previousLeaf);

		const before = readFileSync(firstFile, "utf8");
		mkdirSync(`${firstFile}.lock`);
		expect(() => manager.appendCustomEntry("blocked-append")).toThrow();
		expect(manager.getLeafId()).toBe(previousLeaf);
		expect(readFileSync(firstFile, "utf8")).toBe(before);
		rmSync(`${firstFile}.lock`, { recursive: true, force: true });
		const nextId = manager.appendCustomEntry("successful-append");
		const reopened = SessionManager.open(firstFile);
		expect(reopened.getEntry(nextId)?.parentId).toBe(previousLeaf);
	});

	it("separates a valid unterminated final record before an ordinary append", () => {
		const file = createSessionFile([messageEntry("message-one", null, "one")]);
		const unterminated = readFileSync(file, "utf8").slice(0, -1);
		writeFileSync(file, unterminated);
		const manager = SessionManager.open(file);

		const appendedId = manager.appendCustomEntry("after-unterminated-record");

		const content = readFileSync(file, "utf8");
		expect(content.startsWith(`${unterminated}\n`)).toBe(true);
		expect(parseSessionEntries(content).map((entry) => (entry as { id?: string }).id)).toContain(appendedId);
	});

	it.each(['{"type":', "not-json"])("fails ordinary append on malformed unterminated tail %s", (tail) => {
		const file = createSessionFile([messageEntry("message-one", null, "one")]);
		appendFileSync(file, tail);
		const manager = SessionManager.open(file);
		const before = readFileSync(file, "utf8");
		const beforeLeaf = manager.getLeafId();

		expect(() => manager.appendCustomEntry("must-not-append")).toThrow(/malformed final JSONL entry/);
		expect(readFileSync(file, "utf8")).toBe(before);
		expect(manager.getLeafId()).toBe(beforeLeaf);
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
