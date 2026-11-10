# Standing engineering rule verification

Date: 2026-11-11. Documentation-only change based on merged main `b6e725d`;
the containing commit identifies the change. Founder authorization: make
ongoing evaluation of established approaches a repository rule and update the
local plan.

Added a feature review template and linked the evaluation process from the
roadmap and reuse plan. Local agent instructions and the local blueprint are
kept outside version control. The rule requires relevant primary-source
research, source revisions/dates, reuse evaluation, trade-offs, validation and
PR evidence. It permits citing prior applicable research for small fixes and
keeps urgent repairs and documentation updates proportionate.

Validation: `git diff --check` passes; relative Markdown file links in the four
changed public documents resolve. Manually checked that future-gate authority,
ADR acceptance and provenance requirements are preserved. No upstream source
or dependency was imported, and no runtime or license change is included.
Cargo tests were not rerun for this documentation-only change. The two
pre-existing staged images are excluded from the commit.
