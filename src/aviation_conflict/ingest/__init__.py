"""Bronze: fetch from source APIs and persist raw bytes to data/bronze/.

Append-only, never edited. Stores the response exactly as received under a `_probe` envelope
recording the URL, status, and capture time.

Bronze is not a convenience layer here. OpenSky's /states/all returns only the current
snapshot -- anonymous access has no history and credentials give one hour -- so an uncaptured
snapshot is unrecoverable. This is the only copy that will ever exist.
"""
