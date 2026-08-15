# Copilot agent workflow

This repository uses three project-scoped custom agents:

1. **Planner** inspects the request and produces an implementation-ready plan and acceptance criteria.
2. **Implementer v2** makes the approved changes and runs the relevant verification.
3. **Reviewer** independently checks the resulting diff and reports actionable findings without editing it.

Select the appropriate profile from the agent dropdown when starting a GitHub Copilot task. Pass the planner's output to Implementer v2, then give the issue, plan, and resulting pull request to Reviewer.

## Implementer migration

Use `implementer-v2` for all new tasks. Existing Copilot-created pull requests remain pinned to the custom-agent profile version with which their original task started, so follow-up `@copilot` comments continue with that version. Do not close or recreate active pull requests solely to adopt Implementer v2.

After all pull requests created by the former implementer are merged or closed, its old profile can be removed if it still exists at repository, organization, or enterprise scope.

Agent profiles must be committed to the selected branch and merged into the default branch before they are available to new Copilot cloud-agent tasks from that branch.
