# Override check_perf_event_paranoid to be a no-op inside Docker containers,
# where kernel.perf_event_paranoid cannot be set without --privileged.
# CPU-cycle columns in TPut.csv will be absent; TPut (TX/s) is unaffected.
.PHONY: check_perf_event_paranoid
check_perf_event_paranoid:
	@true
