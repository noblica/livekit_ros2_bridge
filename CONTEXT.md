# Context

Canonical language for the `livekit_ros2_bridge` project. Terms resolved in the
2026-08-25 "Other Audio Sources" session and captured here so the codebase,
docs, and PRDs share one vocabulary.

## Glossary

- **Other Audio Source** — a non-ROS audio origin configured on the bridge (a
  GStreamer source fragment), requested as kind `other_audio`, delivered as one
  **mono audio track**; bypasses ROS access rules; the bridge owns the source.
- **Mono Audio Track** — one LiveKit audio track carrying exactly one source's
  PCM as a single channel; left/right operator audio is achieved by clients
  routing two mono tracks, never by a combined track.
- **Other Video Source** — a non-ROS video origin configured on the bridge (a
  GStreamer source fragment), requested as kind `other_video`, delivered as one
  video track; bypasses ROS access rules; the bridge owns the source.

## Resolved Ambiguities

- **"audio channel"** meant both an operator-selectable feed and interleaved PCM
  channel count; resolved as one Other Audio Source per feed, downmixed to mono
  at the bridge edge.
