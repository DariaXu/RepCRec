_LOCK_MODES = range(2)
R_LOCK, RW_LOCK = _LOCK_MODES

_LOCK_STATES = {
	R_LOCK: 'R',
	RW_LOCK: 'W',
}