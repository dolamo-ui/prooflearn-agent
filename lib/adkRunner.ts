/* The ADK runner (browser side): runAgentsAdk(goal, state) -> a stream of events, exactly like runAgents() in lib/runAgents.ts.

   The agent team (Google ADK) runs on the SERVER. This file only:
     1. starts a run                          POST /api/colleague/adk/start
     2. asks for new events about once a second   GET  /api/colleague/adk/events/<run>?after=N
     3. does the few jobs that only the teacher's browser can do (a "client_call"):
          share_test    publish the test with the teacher passcode and keep the secret teacher key on THIS device
          read_results  read the totals of a test shared from this device (needs that secret key)
        and sends the answer back              POST /api/colleague/adk/reply/<run>
     4. passes Stop on                          POST /api/colleague/adk/stop/<run>
   It then builds the "who did what" ledger from the events (lib/agents.ts), so the screen, the lanes and the PDF/Word report
   need no change. The secret teacher key and the passcode never go to the agent server. */

import { api, loadWork, saveWork } from "./client";
import {
  buildLedger, handoffProblem, headline, isAgentId, type AgentEvent, type AgentEventBody, type AgentRunner, type EmailDraft, type Handoff, type Outcome,
} from "./agents";
import { isLocalAddress, shareUrl } from "./share";
import { fromServer, toServer } from "../components/BlockEditor";

export type AdkStatus = { available: boolean; ready: boolean; model?: string; needsPasscode: boolean; reason: string };

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const str = (v: unknown, max: number) => (typeof v === "string" ? v.trim().slice(0, max) : "");

async function getJson<T>(path: string): Promise<T> {
  let res: Response;
  try { res = await fetch(path, { cache: "no-store" }); } catch { throw new Error("I could not reach the server. Check your internet and try again."); }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || "Something went wrong. Please try again.");
  return data as T;
}

/** Is the agent team available on this server? Never throws: "not available" is a normal answer. */
export async function adkStatus(): Promise<AdkStatus> {
  try { return await getJson<AdkStatus>("/api/colleague/adk/status"); }
  catch { return { available: false, ready: false, needsPasscode: false, reason: "" }; }
}

/* ---------------- the jobs only this browser can do ---------------- */
type ClientCall = { type: "client_call"; call: string; tool: string; args: Record<string, unknown>; seq: number; at: number };

async function doClientCall(c: ClientCall, passcode: string): Promise<{ ok: true; data: object } | { ok: false; error: string }> {
  try {
    if (c.tool === "share_test") {
      const title = str(c.args.title, 120) || "Test from my colleague";
      const questions = Array.isArray(c.args.questions) ? c.args.questions : [];
      if (!questions.length) throw new Error("There are no questions to share.");
      const r = await api<{ code: string; editKey: string }>("/api/work", {
        action: "create", passcode, title, kind: "test", instructions: "", showAnswers: true,
        questions: questions.map((q: any) => toServer(fromServer(q))),
      });
      saveWork([{ code: r.code, key: r.editKey, title, kind: "test" }, ...loadWork().filter((w) => w.code !== r.code)]);
      return { ok: true, data: { code: r.code, title } };      // the secret key stays here
    }
    if (c.tool === "read_results") {
      const saved = loadWork();
      if (!saved.length) throw new Error("No test has been shared from this device yet.");
      const asked = str(c.args.test, 120), current = str(c.args.current, 20);
      const norm = (t: string) => t.toLowerCase().replace(/[^a-z0-9]/g, "");
      let w = saved[0];
      if (asked) {
        const found = saved.find((x) => norm(x.code) === norm(asked) || x.title.toLowerCase().includes(asked.toLowerCase()));
        if (!found) throw new Error(`No shared test matches "${asked.slice(0, 60)}". The tests on this device are: ${saved.slice(0, 5).map((x) => x.title).join("; ")}.`);
        w = found;
      } else if (current) {
        w = saved.find((x) => norm(x.code) === norm(current)) || w;
      }
      const r = await api<{ title: string; count: number; average: number; weakTopics: string[]; questions: { text: string; pct: number }[] }>(
        "/api/work", { action: "results", code: w.code, editKey: w.key });
      // Totals only: learner names and written answers never leave the results screen.
      return { ok: true, data: { title: r.title, count: r.count, average: r.average, weakTopics: r.weakTopics, questions: r.questions.map((q) => ({ text: q.text, pct: q.pct })) } };
    }
    throw new Error("Unknown job.");
  } catch (e: any) {
    return { ok: false, error: String(e?.message || "That step failed.").slice(0, 200) };
  }
}

/** Only events the screen knows how to show get through. */
const SHOWN = new Set(["goal", "plan", "think", "act", "result", "finding", "handoff", "note", "done", "error"]);
const acceptable = (e: any): e is AgentEvent => {
  if (!e || typeof e !== "object" || !SHOWN.has(e.type) || typeof e.seq !== "number") return false;
  if ("agent" in e && !isAgentId(e.agent)) return false;
  if (e.type === "handoff") return handoffProblem(e.handoff) === null;
  return true;
};

export const runAgentsAdk: AgentRunner = async function* (goal, state) {
  const log: AgentEvent[] = [];
  let seq = 0;
  const stamp = (e: AgentEventBody): AgentEvent => ({ ...e, seq: ++seq + 100000, at: Date.now() }) as AgentEvent;

  const started = await api<{ run: string }>("/api/colleague/adk/start", {
    goal, notes: state.notes, level: state.level, maxSteps: state.maxSteps, canShare: state.canShare, passcode: state.passcode,
    savedTests: loadWork().slice(0, 10).map((w) => w.title),
  });
  const run = encodeURIComponent(started.run);
  let after = 0, stopSent = false, finalOutcome: any = null, runError = "";

  for (let idle = 0; ; ) {
    if (state.shouldStop?.() && !stopSent) { stopSent = true; api(`/api/colleague/adk/stop/${run}`, {}).catch(() => undefined); }
    let poll: { events: any[]; done: boolean };
    try { poll = await getJson(`/api/colleague/adk/events/${run}?after=${after}`); }
    catch (e: any) { if (++idle > 5) { runError = e?.message || "I lost contact with the agent team."; break; } await sleep(1000); continue; }
    idle = 0;
    for (const ev of poll.events) {
      after = Math.max(after, ev.seq);
      if (ev.type === "client_call") {
        const answer = await doClientCall(ev as ClientCall, state.passcode);
        await api(`/api/colleague/adk/reply/${run}`, { call: ev.call, ...answer }).catch(() => undefined);
      } else if (ev.type === "final") {
        finalOutcome = ev.outcome;
      } else if (acceptable(ev)) {
        // keep the server's own numbering so lanes keep their order
        log.push(ev);
        yield ev;
      }
    }
    if (poll.done) break;
    await sleep(poll.events.length ? 250 : 800);
  }

  if (runError) yield stamp({ type: "error", text: runError });
  const o = finalOutcome || {};
  const handoffs = log.filter((e): e is AgentEvent & { type: "handoff" } => e.type === "handoff").map((e) => e.handoff as Handoff);
  const ledger = buildLedger(log);
  // The link and code are added here (the AI never writes them). Only for emails drafted after the test was shared.
  const emails: EmailDraft[] = (Array.isArray(o.emails) ? o.emails : []).map((m: any) => {
    let body = str(m.body, 6000);
    if (m.withLink && o.shared?.code) {
      body += `\n\nOpen the test here: ${shareUrl(o.shared.code)}\nOr type the code ${o.shared.code} on the ProofLearn home page. No account is needed.`;
    }
    return { to: str(m.to, 20) || "learners", subject: str(m.subject, 100), body };
  });
  if (o.shared && emails.length && isLocalAddress()) {
    yield stamp({ type: "note", agent: "messenger", level: "warn", text: "The link in this email only opens on this computer. Put ProofLearn online and set NEXT_PUBLIC_APP_URL before you send it to anyone (see README)." });
  }
  const outcome: Outcome = {
    course: o.course || null, questions: Array.isArray(o.questions) ? o.questions : [], plan: o.plan || null, approved: !!o.approved,
    summary: str(o.summary, 900) || "The run ended before a report was written.", stopped: !!o.stopped, steps: Number(o.steps) || 0,
    sources: Array.isArray(o.sources) ? o.sources : [], shared: o.shared || null, results: o.results || null, emails,
    ledger, handoffs, headline: headline(ledger, handoffs), engine: "adk",
  };
  yield stamp({ type: "end", outcome });
};
