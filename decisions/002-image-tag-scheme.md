# 002: Image tag scheme: sha-<short commit sha>, immutable

Stage: S0 · Date: 2026-11-21 · Author: UserAkku · Drafted by agent: yes

## Choice

A deployable image is `ghcr.io/<owner>/<repo>/app:sha-<first 7 chars of the commit sha>`, built once by CI from that commit on `main` and never re-tagged. The image also carries the full sha (`GIT_SHA`, shown by `/version`). A rollback points at an earlier `sha-` tag: `gh workflow run cd.yaml -f image_tag=sha-<old>`. No `latest` tag.

## Rejected alternative

A mutable `latest` tag (alone, or next to the sha tag) that CD always deploys.

## Why

- Immutable tags make "what is running?" answerable: the tag maps to exactly one commit and one image, forever. With `latest`, the same tag means a different image after every push, so neither the host's `docker ps` nor an incident timeline says which code was live, and a host that restarts or re-pulls can silently pick up a newer image than the one that was tested.
- Rollback needs the old image to still have a name. With only `latest`, the previous image's name is gone the moment the new one is pushed.
- The tag is derived from the commit, so CD needs no extra state to know what to deploy: CI's `head_sha` gives the tag. The S1-S7 workflows given in this repo (`platform.yaml`, `serving.yaml`, `mcp.yaml`) use the same `sha-<7>` scheme, so every image in the system reads the same way.
- Semver would need a manual bump per release, which fights deploy-on-every-push; a run number isn't tied to a commit and changes when the same commit is rebuilt.

Accepted trade-off: tags aren't human-friendly or ordered; finding "the previous good one" means looking at `main`'s history or the CD run list.

## What would show this was wrong

- A 7-character prefix collision between two commits (git would start printing longer short shas).
- Needing to rebuild the same commit with a different base image (security patch) and deploy it: one commit would then need two images, which a pure sha tag can't tell apart.
- Rollbacks that are slow or error-prone because nobody can tell which `sha-` tag was last good.
