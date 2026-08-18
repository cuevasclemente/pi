import {
	linkSync,
	lstatSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	symlinkSync,
	truncateSync,
	writeFileSync,
} from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { pathToFileURL } from "url";
import { Worker } from "worker_threads";
import { afterEach, describe, expect, it } from "vitest";
import {
	CURRENT_SESSION_VERSION,
	MAX_DESTRUCTIVE_TRANSACTION_BYTES,
	type SessionEntryBase,
	SessionEntryReplacementCommittedError,
	type SessionHeader,
	SessionManager,
	StaleSessionMutationEpochError,
} from "../src/core/session-manager.ts";

interface SyntheticEntry extends SessionEntryBase {
	value: string;
}

const tempDirs: string[] = [];

interface ReplacementWorkerResult {
	result?: boolean;
	error?: string;
	code?: string;
	heartbeatObserved?: boolean;
	lockIdentityStable?: boolean;
	tempContainedExpected?: boolean;
}

async function createReplacementWorker(data: object): Promise<{
	run: () => Promise<ReplacementWorkerResult>;
}> {
	const sessionManagerUrl = pathToFileURL(join(import.meta.dirname, "../src/core/session-manager.ts")).href;
	const rootTsconfig = join(import.meta.dirname, "../../../tsconfig.json");
	const source = `
		import { appendFileSync, existsSync, readFileSync, readdirSync, statSync } from "node:fs";
		import { basename, dirname } from "node:path";
		import { parentPort, workerData } from "node:worker_threads";
		import { SessionManager } from ${JSON.stringify(sessionManagerUrl)};
		const wait = new Int32Array(new SharedArrayBuffer(4));
		const manager = workerData.mode === "replace" || workerData.mode === "append"
			? SessionManager.open(workerData.file)
			: undefined;
		parentPort.postMessage("ready");
		parentPort.once("message", () => {
			try {
				if (workerData.mode === "replace") {
					parentPort.postMessage({ result: manager.replaceEntriesIfCurrent(workerData.changes) });
					return;
				}
				if (workerData.mode === "append") {
					manager.appendSessionInfo(workerData.name);
					parentPort.postMessage({ result: true });
					return;
				}
				if (workerData.mode === "observeHeartbeat") {
					const lockPath = workerData.file + ".lock";
					const deadline = Date.now() + 10000;
					while (!existsSync(lockPath) && Date.now() < deadline) Atomics.wait(wait, 0, 0, 1);
					if (!existsSync(lockPath)) throw new Error("session lock was not observed");
					const initialStats = statSync(lockPath, { bigint: true });
					while (existsSync(lockPath) && Date.now() < deadline) {
						const currentStats = statSync(lockPath, { bigint: true });
						if (currentStats.mtimeNs !== initialStats.mtimeNs) {
							parentPort.postMessage({
								result: true,
								heartbeatObserved: true,
								lockIdentityStable: currentStats.dev === initialStats.dev && currentStats.ino === initialStats.ino,
							});
							return;
						}
						Atomics.wait(wait, 0, 0, 1);
					}
					throw new Error("session lock heartbeat was not observed");
				}
				const prefix = basename(workerData.file) + ".rewrite-";
				const deadline = Date.now() + 10000;
				while (Date.now() < deadline) {
					const temporaryName = readdirSync(dirname(workerData.file)).find((name) =>
						name.startsWith(prefix),
					);
					if (temporaryName) {
						const temporary = dirname(workerData.file) + "/" + temporaryName;
						const tempContainedExpected = readFileSync(temporary, "utf8").includes(workerData.expectedText);
						appendFileSync(workerData.file, workerData.line);
						parentPort.postMessage({ result: true, tempContainedExpected });
						return;
					}
					Atomics.wait(wait, 0, 0, 1);
				}
				throw new Error("replacement temporary file was not observed");
			} catch (error) {
				parentPort.postMessage({
					error: error instanceof Error ? error.message : String(error),
					code: typeof error === "object" && error !== null && "code" in error ? String(error.code) : undefined,
				});
			}
		});
	`;
	const worker = new Worker(new URL(`data:text/javascript,${encodeURIComponent(source)}`), {
		workerData: data,
		execArgv: ["--import", "tsx"],
		env: { ...process.env, TSX_TSCONFIG_PATH: rootTsconfig },
	});
	await new Promise<void>((resolve, reject) => {
		worker.once("message", (message: unknown) => {
			if (message === "ready") resolve();
			else reject(new Error(`Unexpected worker readiness message: ${String(message)}`));
		});
		worker.once("error", reject);
	});
	return {
		run: async () => {
			const result = await new Promise<ReplacementWorkerResult>((resolve, reject) => {
				worker.once("message", (message: unknown) => resolve(message as ReplacementWorkerResult));
				worker.once("error", reject);
				worker.postMessage("start");
			});
			await worker.terminate();
			return result;
		},
	};
}

async function createSuccessorLockWorker(
	file: string,
	trigger: "temporary" | "canonicalRename" = "temporary",
): Promise<{
	acquire: () => Promise<{ dev: string; ino: string }>;
	release: () => Promise<{ existsAfterRelease: boolean }>;
}> {
	const rootTsconfig = join(import.meta.dirname, "../../../tsconfig.json");
	const requireBase = pathToFileURL(join(import.meta.dirname, "successor-lock-worker.cjs")).href;
	const source = `
		import { existsSync, lstatSync, mkdirSync, readdirSync, rmdirSync } from "node:fs";
		import { createRequire } from "node:module";
		import { basename, dirname } from "node:path";
		import { parentPort, workerData } from "node:worker_threads";
		const require = createRequire(workerData.requireBase);
		const lockfile = require("proper-lockfile");
		const wait = new Int32Array(new SharedArrayBuffer(4));
		const initialFileStats = lstatSync(workerData.file, { bigint: true });
		let successorRelease;
		parentPort.postMessage("ready");
		parentPort.on("message", (message) => {
			try {
				if (message === "acquire") {
					const deadline = Date.now() + 10000;
					if (workerData.trigger === "temporary") {
						const prefix = basename(workerData.file) + ".rewrite-";
						while (Date.now() < deadline) {
							if (readdirSync(dirname(workerData.file)).some((name) => name.startsWith(prefix))) break;
							Atomics.wait(wait, 0, 0, 1);
						}
					} else {
						while (Date.now() < deadline) {
							const currentFileStats = lstatSync(workerData.file, { bigint: true });
							if (currentFileStats.dev !== initialFileStats.dev || currentFileStats.ino !== initialFileStats.ino) break;
							Atomics.wait(wait, 0, 0, 1);
						}
					}
					const lockPath = workerData.file + ".lock";
					rmdirSync(lockPath);
					const inodeGuard = lockPath + ".inode-guard";
					mkdirSync(inodeGuard);
					successorRelease = lockfile.lockSync(workerData.file, {
						stale: 300000,
						update: 150000,
						retries: 0,
					});
					const stats = lstatSync(lockPath, { bigint: true });
					rmdirSync(inodeGuard);
					parentPort.postMessage({ dev: String(stats.dev), ino: String(stats.ino) });
					return;
				}
				successorRelease();
				parentPort.postMessage({ existsAfterRelease: existsSync(workerData.file + ".lock") });
			} catch (error) {
				parentPort.postMessage({ error: error instanceof Error ? error.message : String(error) });
			}
		});
	`;
	const worker = new Worker(new URL(`data:text/javascript,${encodeURIComponent(source)}`), {
		workerData: { file, requireBase, trigger },
		execArgv: ["--import", "tsx"],
		env: { ...process.env, TSX_TSCONFIG_PATH: rootTsconfig },
	});
	await new Promise<void>((resolve, reject) => {
		worker.once("message", (message: unknown) => {
			if (message === "ready") resolve();
			else reject(new Error(`Unexpected successor worker readiness message: ${String(message)}`));
		});
		worker.once("error", reject);
	});
	return {
		acquire: () =>
			new Promise<{ dev: string; ino: string }>((resolve, reject) => {
				worker.once("message", (message: unknown) => {
					if (typeof message === "object" && message !== null && "error" in message) {
						reject(new Error(String(message.error)));
					} else {
						resolve(message as { dev: string; ino: string });
					}
				});
				worker.once("error", reject);
				worker.postMessage("acquire");
			}),
		release: async () => {
			const result = await new Promise<{ existsAfterRelease: boolean }>((resolve, reject) => {
				worker.once("message", (message: unknown) => {
					if (typeof message === "object" && message !== null && "error" in message) {
						reject(new Error(String(message.error)));
					} else {
						resolve(message as { existsAfterRelease: boolean });
					}
				});
				worker.once("error", reject);
				worker.postMessage("release");
			});
			await worker.terminate();
			return result;
		},
	};
}

function createSessionFile(lines?: string[]): {
	dir: string;
	file: string;
	header: SessionHeader;
	root: SyntheticEntry;
	left: SyntheticEntry;
	right: SyntheticEntry;
} {
	const dir = mkdtempSync(join(tmpdir(), "pi-entry-replacement-"));
	tempDirs.push(dir);
	const file = join(dir, "session.jsonl");
	const header: SessionHeader = {
		type: "session",
		version: CURRENT_SESSION_VERSION,
		id: "synthetic-session",
		timestamp: "2026-01-01T00:00:00.000Z",
		cwd: dir,
	};
	const root: SyntheticEntry = {
		type: "synthetic",
		id: "root",
		parentId: null,
		timestamp: "2026-01-01T00:00:01.000Z",
		value: "old-secret",
	};
	const left: SyntheticEntry = {
		type: "synthetic",
		id: "left",
		parentId: root.id,
		timestamp: "2026-01-01T00:00:02.000Z",
		value: "left",
	};
	const right: SyntheticEntry = {
		type: "synthetic",
		id: "right",
		parentId: root.id,
		timestamp: "2026-01-01T00:00:03.000Z",
		value: "right",
	};
	writeFileSync(
		file,
		lines?.join("") ?? [header, root, left, right].map((entry) => `${JSON.stringify(entry)}\n`).join(""),
	);
	return { dir, file, header, root, left, right };
}

afterEach(() => {
	for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe("SessionManager.replaceEntryIfCurrent", () => {
	it("replaces payloads and event types without changing the selected branch", () => {
		const { file, header, root } = createSessionFile();
		const manager = SessionManager.open(file);
		manager.branch("left");

		const payloadReplacement: SyntheticEntry = { ...root, value: "new-value" };
		expect(manager.replaceEntryIfCurrent(root, payloadReplacement)).toBe(true);
		expect(manager.getLeafId()).toBe("left");
		expect(manager.getBranch().map((entry) => entry.id)).toEqual(["root", "left"]);
		expect(manager.getEntry("root")).toEqual(payloadReplacement);

		const tombstone = {
			type: "wayang_tombstone",
			id: root.id,
			parentId: root.parentId,
			timestamp: "2026-01-01T00:00:04.000Z",
		};
		expect(manager.replaceEntryIfCurrent(payloadReplacement, tombstone)).toBe(true);
		expect(manager.getLeafId()).toBe("left");
		expect(manager.getEntry("root")).toEqual(tombstone);
		expect(manager.getHeader()).toEqual({ ...header, mutationEpoch: 2 });
		expect(readFileSync(file, "utf8")).not.toContain("old-secret");
		expect(readFileSync(file, "utf8")).not.toContain("new-value");
		expect(readdirSync(join(file, ".."))).toEqual(["session.jsonl"]);
	});

	it("publishes multiple replacements atomically and refuses a partially stale set", () => {
		const { file, root, left } = createSessionFile();
		const manager = SessionManager.open(file);
		const originalBytes = readFileSync(file);
		const rootTombstone = { ...root, type: "wayang_tombstone", value: "root-deleted" };
		const leftTombstone = { ...left, type: "wayang_tombstone", value: "left-deleted" };

		expect(
			manager.replaceEntriesIfCurrent([
				{ expectedEntry: root, replacement: rootTombstone },
				{ expectedEntry: { ...left, value: "stale" }, replacement: leftTombstone },
			]),
		).toBe(false);
		expect(readFileSync(file)).toEqual(originalBytes);
		expect(manager.getEntry(root.id)).toEqual(root);

		expect(
			manager.replaceEntriesIfCurrent([
				{ expectedEntry: root, replacement: rootTombstone },
				{ expectedEntry: left, replacement: leftTombstone },
			]),
		).toBe(true);
		expect(manager.getEntry(root.id)).toEqual(rootTombstone);
		expect(manager.getEntry(left.id)).toEqual(leftTombstone);
		const physical = readFileSync(file, "utf8");
		expect(physical).toContain("root-deleted");
		expect(physical).toContain("left-deleted");
		expect(physical).not.toContain("old-secret");
	});

	it("matches the JSON form held by a manager that materialized the file", () => {
		const dir = mkdtempSync(join(tmpdir(), "pi-entry-replacement-created-"));
		tempDirs.push(dir);
		const manager = SessionManager.create(dir, dir, { id: "materialized-session" });
		const rootId = manager.appendCustomEntry("synthetic");
		const expected = structuredClone(manager.getEntry(rootId)!);
		manager.materialize();
		const tombstone = {
			type: "wayang_tombstone",
			id: expected.id,
			parentId: expected.parentId,
			timestamp: expected.timestamp,
		};

		expect(manager.replaceEntryIfCurrent(expected, tombstone)).toBe(true);
		expect(manager.getEntry(rootId)).toEqual(tombstone);
	});

	it("preserves an explicitly selected branch in an in-memory session", () => {
		const manager = SessionManager.inMemory("/synthetic");
		const rootId = manager.appendCustomEntry("synthetic", { value: "old" });
		const root = structuredClone(manager.getEntry(rootId)!);
		const leftId = manager.appendCustomEntry("synthetic", { value: "left" });
		manager.branch(rootId);
		manager.appendCustomEntry("synthetic", { value: "right" });
		manager.branch(leftId);
		const replacement = { ...root, type: "wayang_tombstone", data: undefined };

		expect(manager.replaceEntryIfCurrent(root, replacement)).toBe(true);
		expect(manager.getLeafId()).toBe(leftId);
		expect(manager.getBranch().map((entry) => entry.id)).toEqual([rootId, leftId]);
	});

	it("refuses a stale exact-entry CAS without changing bytes or manager state", () => {
		const { file, root } = createSessionFile();
		const winner = SessionManager.open(file);
		const stale = SessionManager.open(file);
		const winnerReplacement = { ...root, value: "winner" };
		expect(winner.replaceEntryIfCurrent(root, winnerReplacement)).toBe(true);
		const bytesAfterWinner = readFileSync(file);
		const staleEntries = structuredClone(stale.getEntries());
		const staleLeaf = stale.getLeafId();

		expect(stale.replaceEntryIfCurrent(root, { ...root, value: "loser" })).toBe(false);
		expect(readFileSync(file)).toEqual(bytesAfterWinner);
		expect(stale.getEntries()).toEqual(staleEntries);
		expect(stale.getLeafId()).toBe(staleLeaf);
		expect(readdirSync(join(file, "..")).filter((name) => name.includes(".rewrite-"))).toEqual([]);
	});

	it("rejects header and topology replacement without changing state", () => {
		const { file, header, root } = createSessionFile();
		const manager = SessionManager.open(file);
		const originalBytes = readFileSync(file);
		const originalEntries = structuredClone(manager.getEntries());

		expect(() =>
			manager.replaceEntryIfCurrent(header as unknown as SessionEntryBase, {
				type: "wayang_tombstone",
				id: header.id,
				parentId: null,
				timestamp: header.timestamp,
			}),
		).toThrow(/header/i);
		expect(() => manager.replaceEntryIfCurrent(root, { ...root, id: "changed" })).toThrow(/ID is immutable/);
		expect(() => manager.replaceEntryIfCurrent(root, { ...root, parentId: "changed" })).toThrow(
			/parentId is immutable/,
		);
		expect(() => manager.replaceEntryIfCurrent(root, { ...root, type: "session" })).toThrow(/header/i);
		expect(readFileSync(file)).toEqual(originalBytes);
		expect(manager.getEntries()).toEqual(originalEntries);
	});

	it("refuses a physically changed header without changing bytes or manager state", () => {
		const { file, header, root, left, right } = createSessionFile();
		const manager = SessionManager.open(file);
		const originalState = structuredClone(manager.getEntries());
		const changedHeader = { ...header, cwd: "/independently-changed" };
		const changedBytes = [changedHeader, root, left, right].map((entry) => `${JSON.stringify(entry)}\n`).join("");
		writeFileSync(file, changedBytes);

		expect(() => manager.replaceEntryIfCurrent(root, { ...root, value: "new" })).toThrow(/header changed/);
		expect(readFileSync(file, "utf8")).toBe(changedBytes);
		expect(manager.getEntries()).toEqual(originalState);
	});

	it.each([
		{
			name: "malformed",
			build: (header: SessionHeader, root: SyntheticEntry) =>
				`${JSON.stringify(header)}\n${JSON.stringify(root)}\n{malformed}\n`,
			error: /malformed JSONL entry/,
		},
		{
			name: "unterminated",
			build: (header: SessionHeader, root: SyntheticEntry) => `${JSON.stringify(header)}\n${JSON.stringify(root)}`,
			error: /unterminated JSONL tail/,
		},
		{
			name: "duplicate-entry",
			build: (header: SessionHeader, root: SyntheticEntry) =>
				`${JSON.stringify(header)}\n${JSON.stringify(root)}\n${JSON.stringify(root)}\n`,
			error: /duplicate entry ID/,
		},
	])("rejects a $name physical file without rewriting it", ({ build, error }) => {
		const fixture = createSessionFile();
		const content = build(fixture.header, fixture.root);
		writeFileSync(fixture.file, content);
		const manager = SessionManager.open(fixture.file);
		const originalState = structuredClone(manager.getEntries());

		expect(() => manager.replaceEntryIfCurrent(fixture.root, { ...fixture.root, value: "new" })).toThrow(error);
		expect(readFileSync(fixture.file, "utf8")).toBe(content);
		expect(manager.getEntries()).toEqual(originalState);
	});

	it("refuses physical input above the destructive transaction byte bound", () => {
		const fixture = createSessionFile();
		const manager = SessionManager.open(fixture.file);
		const originalState = structuredClone(manager.getEntries());
		truncateSync(fixture.file, MAX_DESTRUCTIVE_TRANSACTION_BYTES + 1);

		expect(() => manager.replaceEntryIfCurrent(fixture.root, { ...fixture.root, value: "new" })).toThrow(
			/256 MiB destructive transaction limit/,
		);
		expect(manager.getEntries()).toEqual(originalState);
		expect(readdirSync(fixture.dir).filter((name) => name.includes(".rewrite-"))).toEqual([]);
	});

	it(
		"refreshes the same lock-directory lease throughout long synchronous chunk loops",
		async () => {
			const fixture = createSessionFile();
			const manager = SessionManager.open(fixture.file);
			const paddingEntries = Array.from({ length: 24 }, (_, index) => ({
				type: "synthetic",
				id: `padding-${index}`,
				parentId: index === 0 ? fixture.right.id : `padding-${index - 1}`,
				timestamp: "2026-01-01T00:00:05.000Z",
				value: "x".repeat(1024 * 1024),
			}));
			writeFileSync(
				fixture.file,
				[fixture.header, fixture.root, fixture.left, fixture.right, ...paddingEntries]
					.map((entry) => `${JSON.stringify(entry)}\n`)
					.join(""),
			);
			const observer = await createReplacementWorker({ mode: "observeHeartbeat", file: fixture.file });
			const observerResult = observer.run();

			expect(manager.replaceEntryIfCurrent(fixture.root, { ...fixture.root, value: "replacement" })).toBe(true);
			expect(await observerResult).toEqual({
				result: true,
				heartbeatObserved: true,
				lockIdentityStable: true,
			});
			expect(manager.getEntry(fixture.root.id)).toMatchObject({ value: "replacement" });
		},
		20000,
	);

	it(
		"does not remove a successor lock after destructive lease ownership is lost",
		async () => {
			const fixture = createSessionFile();
			const manager = SessionManager.open(fixture.file);
			const paddingEntries = Array.from({ length: 16 }, (_, index) => ({
				type: "synthetic",
				id: `successor-padding-${index}`,
				parentId: index === 0 ? fixture.right.id : `successor-padding-${index - 1}`,
				timestamp: "2026-01-01T00:00:05.000Z",
				value: "x".repeat(1024 * 1024),
			}));
			writeFileSync(
				fixture.file,
				[fixture.header, fixture.root, fixture.left, fixture.right, ...paddingEntries]
					.map((entry) => `${JSON.stringify(entry)}\n`)
					.join(""),
			);
			const successor = await createSuccessorLockWorker(fixture.file);
			const acquiredSuccessor = successor.acquire();

			expect(() => manager.replaceEntryIfCurrent(fixture.root, { ...fixture.root, value: "replacement" })).toThrow(
				/(?:ownership was lost|Refusing to remove)/,
			);
			const successorIdentity = await acquiredSuccessor;
			const survivingStats = lstatSync(`${fixture.file}.lock`, { bigint: true });
			expect({ dev: String(survivingStats.dev), ino: String(survivingStats.ino) }).toEqual(successorIdentity);
			expect(manager.getEntry(fixture.root.id)).toEqual(fixture.root);
			expect(readdirSync(fixture.dir).filter((name) => name.includes(".rewrite-"))).toEqual([]);

			expect(await successor.release()).toEqual({ existsAfterRelease: false });
		},
		20000,
	);

	it(
		"reports a typed committed error while keeping manager and canonical state new",
		async () => {
			const fixture = createSessionFile();
			const manager = SessionManager.open(fixture.file);
			const paddingEntries = Array.from({ length: 16 }, (_, index) => ({
				type: "synthetic",
				id: `post-commit-padding-${index}`,
				parentId: index === 0 ? fixture.right.id : `post-commit-padding-${index - 1}`,
				timestamp: "2026-01-01T00:00:05.000Z",
				value: "x".repeat(1024 * 1024),
			}));
			writeFileSync(
				fixture.file,
				[fixture.header, fixture.root, fixture.left, fixture.right, ...paddingEntries]
					.map((entry) => `${JSON.stringify(entry)}\n`)
					.join(""),
			);
			const successor = await createSuccessorLockWorker(fixture.file, "canonicalRename");
			const acquiredSuccessor = successor.acquire();
			const replacement = { ...fixture.root, value: "committed replacement" };
			let caught: unknown;
			try {
				manager.replaceEntryIfCurrent(fixture.root, replacement);
			} catch (error) {
				caught = error;
			}

			expect(caught).toBeInstanceOf(SessionEntryReplacementCommittedError);
			expect(caught).toMatchObject({ committed: true, code: "ESESSIONREPLACEMENTCOMMITTED" });
			await acquiredSuccessor;
			expect(manager.getEntry(fixture.root.id)).toEqual(replacement);
			expect(manager.getHeader()?.mutationEpoch).toBe(1);
			expect(await successor.release()).toEqual({ existsAfterRelease: false });
			const reopened = SessionManager.open(fixture.file);
			expect(reopened.getEntry(fixture.root.id)).toEqual(replacement);
			expect(reopened.getHeader()?.mutationEpoch).toBe(1);
		},
		20000,
	);

	it("rejects every append surface from a manager made stale by replacement", () => {
		const fixture = createSessionFile();
		const stale = SessionManager.open(fixture.file);
		const replacer = SessionManager.open(fixture.file);
		expect(replacer.replaceEntryIfCurrent(fixture.root, { ...fixture.root, value: "replacement" })).toBe(true);

		const staleWrites: Array<() => unknown> = [
			() => stale.appendMessage({ role: "user", content: "stale", timestamp: Date.now() }),
			() => stale.appendThinkingLevelChange("high"),
			() => stale.appendModelChange("synthetic-provider", "synthetic-model"),
			() => stale.appendCompaction("stale", fixture.root.id, 1),
			() => stale.appendCustomEntry("synthetic", { stale: true }),
			() => stale.appendCustomMessageEntry("synthetic", "stale", false),
			() => stale.appendLabelChange(fixture.left.id, "stale"),
			() => stale.branchWithSummary(fixture.left.id, "stale"),
			() => stale.appendSessionInfo("stale name"),
		];
		for (const write of staleWrites) expect(write).toThrow(StaleSessionMutationEpochError);
		expect(stale.getEntry(fixture.root.id)).toEqual(fixture.root);
		expect(stale.getHeader()?.mutationEpoch).toBeUndefined();

		const reopened = SessionManager.open(fixture.file);
		expect(reopened.appendCustomEntry("synthetic", { reopened: true })).toEqual(expect.any(String));
		expect(reopened.appendSessionInfo("reopened name")).toEqual(expect.any(String));
	});

	it("increments the mutation epoch once per successful multi-replacement from a legacy header", () => {
		const fixture = createSessionFile();
		const manager = SessionManager.open(fixture.file);
		expect(manager.getHeader()?.mutationEpoch).toBeUndefined();
		const rootReplacement = { ...fixture.root, value: "root replacement" };
		const leftReplacement = { ...fixture.left, value: "left replacement" };
		expect(
			manager.replaceEntriesIfCurrent([
				{ expectedEntry: fixture.root, replacement: rootReplacement },
				{ expectedEntry: fixture.left, replacement: leftReplacement },
			]),
		).toBe(true);
		expect(manager.getHeader()?.mutationEpoch).toBe(1);
		expect(manager.replaceEntryIfCurrent(rootReplacement, { ...rootReplacement, value: "second" })).toBe(true);
		expect(manager.getHeader()?.mutationEpoch).toBe(2);
		expect(SessionManager.open(fixture.file).getHeader()?.mutationEpoch).toBe(2);
	});

	it("rejects invalid UTF-8 and invalid timestamps on the destructive path", () => {
		const invalidUtf8 = createSessionFile();
		const validBytes = readFileSync(invalidUtf8.file);
		writeFileSync(invalidUtf8.file, Buffer.concat([validBytes, Buffer.from([0xff, 0x0a])]));
		const utf8Manager = SessionManager.open(invalidUtf8.file);
		expect(() => utf8Manager.replaceEntryIfCurrent(invalidUtf8.root, { ...invalidUtf8.root, value: "new" })).toThrow(
			/invalid UTF-8/,
		);

		const invalidTimestamp = createSessionFile();
		const timestampManager = SessionManager.open(invalidTimestamp.file);
		expect(() =>
			timestampManager.replaceEntryIfCurrent(invalidTimestamp.root, {
				...invalidTimestamp.root,
				timestamp: "not-a-timestamp",
			}),
		).toThrow(/invalid session entry envelope/);

		const invalidPhysicalTimestamp = createSessionFile();
		writeFileSync(
			invalidPhysicalTimestamp.file,
			[
				invalidPhysicalTimestamp.header,
				invalidPhysicalTimestamp.root,
				{ ...invalidPhysicalTimestamp.right, timestamp: "bad" },
			]
				.map((entry) => `${JSON.stringify(entry)}\n`)
				.join(""),
		);
		const physicalTimestampManager = SessionManager.open(invalidPhysicalTimestamp.file);
		expect(() =>
			physicalTimestampManager.replaceEntryIfCurrent(invalidPhysicalTimestamp.root, {
				...invalidPhysicalTimestamp.root,
				value: "new",
			}),
		).toThrow(/malformed session entry/);
	});

	it("preserves an unrelated locked writer revision while replacing the target", () => {
		const { file, root } = createSessionFile();
		const appendingWriter = SessionManager.open(file);
		const replacingWriter = SessionManager.open(file);
		replacingWriter.branch("left");
		appendingWriter.appendSessionInfo("concurrent name");

		expect(replacingWriter.replaceEntryIfCurrent(root, { ...root, value: "replacement" })).toBe(true);
		expect(replacingWriter.getSessionName()).toBe("concurrent name");
		expect(replacingWriter.getLeafId()).toBe("left");
		const reopened = SessionManager.open(file);
		expect(reopened.getEntry("root")).toMatchObject({ value: "replacement" });
		expect(reopened.getSessionName()).toBe("concurrent name");
	});

	it(
		"allows only one concurrent writer to replace the same exact target",
		async () => {
			const { file, root } = createSessionFile();
			const first = await createReplacementWorker({
				mode: "replace",
				file,
				changes: [{ expectedEntry: root, replacement: { ...root, value: "first" } }],
			});
			const second = await createReplacementWorker({
				mode: "replace",
				file,
				changes: [{ expectedEntry: root, replacement: { ...root, value: "second" } }],
			});

			const [firstResult, secondResult] = await Promise.all([first.run(), second.run()]);
			expect(firstResult.error).toBeUndefined();
			expect(secondResult.error).toBeUndefined();
			expect([firstResult.result, secondResult.result].sort()).toEqual([false, true]);
			expect(SessionManager.open(file).getEntry(root.id)).toMatchObject({
				value: expect.stringMatching(/^(first|second)$/),
			});
			expect(readdirSync(join(file, "..")).filter((name) => name.includes(".rewrite-"))).toEqual([]);
		},
		20000,
	);

	it(
		"preserves both concurrent replacements of different targets",
		async () => {
			const { file, root, right } = createSessionFile();
			const rootWriter = await createReplacementWorker({
				mode: "replace",
				file,
				changes: [{ expectedEntry: root, replacement: { ...root, value: "root-replaced" } }],
			});
			const rightWriter = await createReplacementWorker({
				mode: "replace",
				file,
				changes: [{ expectedEntry: right, replacement: { ...right, value: "right-replaced" } }],
			});

			const results = await Promise.all([rootWriter.run(), rightWriter.run()]);
			expect(results).toEqual([{ result: true }, { result: true }]);
			const reopened = SessionManager.open(file);
			expect(reopened.getEntry(root.id)).toMatchObject({ value: "root-replaced" });
			expect(reopened.getEntry(right.id)).toMatchObject({ value: "right-replaced" });
		},
		20000,
	);

	it(
		"preserves a cooperative append racing a replacement commit",
		async () => {
			const { file, root } = createSessionFile();
			const replacer = await createReplacementWorker({
				mode: "replace",
				file,
				changes: [{ expectedEntry: root, replacement: { ...root, value: "replacement" } }],
			});
			const appender = await createReplacementWorker({ mode: "append", file, name: "racing append" });

			const [replacementResult, appendResult] = await Promise.all([replacer.run(), appender.run()]);
			expect(replacementResult).toEqual({ result: true });
			const reopened = SessionManager.open(file);
			expect(reopened.getEntry(root.id)).toMatchObject({ value: "replacement" });
			if (appendResult.result) {
				expect(appendResult).toEqual({ result: true });
				expect(reopened.getSessionName()).toBe("racing append");
			} else {
				expect(appendResult.code).toBe("ESTALESESSIONMUTATIONEPOCH");
				expect(reopened.getSessionName()).toBeUndefined();
				reopened.appendSessionInfo("append after reopen");
				const afterReopenAppend = SessionManager.open(file);
				expect(afterReopenAppend.getEntry(root.id)).toMatchObject({ value: "replacement" });
				expect(afterReopenAppend.getSessionName()).toBe("append after reopen");
			}
		},
		20000,
	);

	it(
		"cleans a failed transaction temp that never contains the replaced payload",
		async () => {
			const fixture = createSessionFile();
			const padding = {
				type: "synthetic",
				id: "padding",
				parentId: fixture.right.id,
				timestamp: "2026-01-01T00:00:05.000Z",
				value: "x".repeat(16 * 1024 * 1024),
			};
			writeFileSync(
				fixture.file,
				[fixture.header, fixture.root, fixture.left, fixture.right, padding]
					.map((entry) => `${JSON.stringify(entry)}\n`)
					.join(""),
			);
			const manager = SessionManager.open(fixture.file);
			const racingEntry = {
				type: "synthetic",
				id: "uncooperative",
				parentId: padding.id,
				timestamp: "2026-01-01T00:00:06.000Z",
				value: "racing",
			};
			const observer = await createReplacementWorker({
				mode: "rawAppendOnTemp",
				file: fixture.file,
				expectedText: fixture.root.value,
				line: `${JSON.stringify(racingEntry)}\n`,
			});
			const observerResult = observer.run();

			expect(() =>
				manager.replaceEntryIfCurrent(fixture.root, {
					...fixture.root,
					type: "wayang_tombstone",
					value: "deleted",
				}),
			).toThrow(/changed before transaction commit/);
			expect(await observerResult).toEqual({ result: true, tempContainedExpected: false });
			expect(readdirSync(fixture.dir).filter((name) => name.includes(".rewrite-"))).toEqual([]);
			expect(manager.getEntry(fixture.root.id)).toEqual(fixture.root);
		},
		20000,
	);

	it("removes crashed rewrite temps on open and before a replacement", () => {
		const fixture = createSessionFile();
		const temporary = `${fixture.file}.rewrite-crashed`;
		writeFileSync(temporary, "new replacement content only");
		const manager = SessionManager.open(fixture.file);
		expect(readdirSync(fixture.dir)).toEqual(["session.jsonl"]);

		writeFileSync(temporary, `stale writer retained ${fixture.root.value}`);
		expect(
			manager.replaceEntryIfCurrent(fixture.root, {
				...fixture.root,
				type: "wayang_tombstone",
				value: "deleted",
			}),
		).toBe(true);
		expect(readdirSync(fixture.dir)).toEqual(["session.jsonl"]);
		expect(readFileSync(fixture.file, "utf8")).not.toContain(fixture.root.value);
	});

	it("resolves symlink aliases and rejects hardlinks with full rollback", () => {
		const aliasFixture = createSessionFile();
		const alias = join(aliasFixture.dir, "alias.jsonl");
		symlinkSync(aliasFixture.file, alias);
		const aliasManager = SessionManager.open(alias);
		expect(aliasManager.replaceEntryIfCurrent(aliasFixture.root, { ...aliasFixture.root, value: "alias" })).toBe(
			true,
		);
		expect(JSON.parse(readFileSync(alias, "utf8").split("\n")[1])).toMatchObject({ value: "alias" });

		const hardlinkFixture = createSessionFile();
		const hardlinkManager = SessionManager.open(hardlinkFixture.file);
		const hardlink = join(hardlinkFixture.dir, "hardlink.jsonl");
		linkSync(hardlinkFixture.file, hardlink);
		const originalBytes = readFileSync(hardlinkFixture.file);
		const originalEntries = structuredClone(hardlinkManager.getEntries());
		const originalLeaf = hardlinkManager.getLeafId();

		expect(() =>
			hardlinkManager.replaceEntryIfCurrent(hardlinkFixture.root, { ...hardlinkFixture.root, value: "rejected" }),
		).toThrow(/multiple hard links/);
		expect(readFileSync(hardlinkFixture.file)).toEqual(originalBytes);
		expect(readFileSync(hardlink)).toEqual(originalBytes);
		expect(hardlinkManager.getEntries()).toEqual(originalEntries);
		expect(hardlinkManager.getLeafId()).toBe(originalLeaf);
		expect(readdirSync(hardlinkFixture.dir).sort()).toEqual(["hardlink.jsonl", "session.jsonl"]);
	});
});
