

# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.
- Create Commits often

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## 5. Explain Like I'm Learning

**The user is learning. Explain every concept at a child-friendly level.**

When introducing or touching any technical concept:
- Use plain, everyday language. Avoid jargon; when a technical term is unavoidable, define it in one short sentence.
- Use simple analogies to ground abstract ideas.
- Break explanations into small steps. One idea per sentence when possible.
- Show *why* something matters before *how* it works.
- After explaining, give a tiny concrete example the user can picture.
- It is okay to be repetitive across sessions — assume the user is still building intuition.

Apply this whenever you:
- Introduce a new library, framework, or pattern.
- Use a term like "state", "API", "async", "component", "hook", "endpoint", etc.
- Make a design decision the user might not have seen before.

## 6. Who I am (calibrate to this)

*(Moved here from PROJECT.md so a new session picks it up automatically.)*

Rising sophomore CS student. Comfortable with Go, Python, Docker, Postgres, Redis, gRPC. Currently doing systems research: formal verification of HBase's region split protocol (TLA+, invariants, rollback correctness) and TLA+ specs for the CORFU shared log at NYU.

So: I understand distributed systems concepts and protocol reasoning reasonably well. I have less experience with production-grade Go services, benchmarking methodology, and object storage APIs. Explain build/tooling decisions more than protocol concepts. Don't hand me finished code without explaining the design choice behind it — I'm building this to learn.

## 7. How we work: explain → agree → build

**No code appears until I've seen the reasoning and signed off on it.** In that order, every step.

- Explain the step first: what it does, every design decision in it, the alternatives, why this one.
- Name tradeoffs on both sides, then recommend. Don't pick silently.
- Wait for my go. Then write the code.
- Verify against the step's stated check. Then commit. Then the next step.
- If an explanation is too big to hold in my head at once, the step is too big — split it.

See section 5 for *how* to explain. This section is about *when*.

## 8. Steps are tiny and independently verifiable

- One new thing per step. If a step has two new things in it, it's two steps.
- Write the success check before the code.
- One commit per verified step. The message says *why*, not just what.
- Never batch several steps into one turn, even when they're each small.

## 9. Verify claims, don't assert them

- Prove tool, API, and library behavior in the terminal before I act on it.
- Prefer a temporary experiment over an assertion — a throwaway flag, a test pointed at a dead port.
- When a check contradicts you, say so plainly, correct the record, and move on.

## 10. Push back, including on the plan

- Argue against the plan file or PROJECT.md when they're wrong. An approved plan is not evidence.
- Flag speculative abstractions even when a plan calls for them.
- Push back if I'm scoping badly or building the wrong thing next.

## 11. Where things live

- `PROJECT.md` — the brief: problem, architecture, data model, milestones, open questions.
- `DECISIONS.md` — settled calls and their reasons. Read it before proposing something different; append to it when a new one is settled.
- `~/.claude/plans/okay-read-through-the-ticklish-lovelace.md` — the M1 step-by-step plan and where we currently are in it.
