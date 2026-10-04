# CS-451 Distributed Algorithms — EPFL, Fall 2026

## Project
Implementing three layered abstractions over raw UDP: Perfect Links -> FIFO
(Uniform Reliable) Broadcast -> Lattice Agreement. Individual project, 20% of
course grade. Deadlines: Perfect Links Oct 30, FIFO Broadcast Dec 9, Lattice
Agreement Dec 30.

## Language & build
- C++17, CMake, using the official mandatory template (do not edit
  CMakeLists.txt's protected sections; all code goes under `src/`).
- Confirmed grading-equivalent toolchain (checked on the official VM):
  Ubuntu 22.04, g++ 11.4.0, cmake 3.22.1.
- Build/run via the template's own scripts: `./build.sh`, `./run.sh --id ID
  --hosts HOSTS --output OUTPUT CONFIG`, `./cleanup.sh` before submission.

## Hard constraints
- No 3rd-party libraries — everything on top of the standard library.
- Communication: raw UDP only, one socket per process, no built-in
  reliability features. Up to 8 app-messages per packet.
- Runtime limits (grading): 2 CPU cores, 4 GiB RAM, max 8 threads/process.
- SIGTERM/SIGINT simulate a crash: stop immediately, only log-writing is
  allowed afterward. SIGSTOP/SIGCONT pause/resume. Template already handles
  signal wiring.
- Output log format: `b seq_nr` (broadcast/send), `d sender seq_nr`
  (deliver) for Milestones 1-2; decided sets (space-separated ints) for
  Milestone 3, one line per instance in config-file order.
- Grading = correctness (3.5/6 floor once tests pass) + performance (2.5/6,
  competitively ranked, separately per language track).

## Current status
- `perfect_links.hpp`: public interface designed (`send()`, `start()`,
  `stop()`, `DeliverCallback` matching the pl.Send/pl.Deliver events).
  Private internals (retransmission bookkeeping, ack tracking, dedup,
  thread model) are deliberately NOT implemented yet — see next section.

## How to help with this project
The course explicitly discourages relying on AI-generated code — the final
exam tests understanding of the actual design choices made, and code
similarity tools are used on submissions. Treat implementation logic
(retransmission strategy, dedup, threading model, the actual algorithms) as
my own work to write: help by discussing tradeoffs, asking clarifying
questions, reviewing/debugging code I've written, and explaining concepts —
not by writing the graded logic outright.