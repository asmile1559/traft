package traft

func (r *raftNode) Recover() error {
	term, votedFor, err := r.persister.LoadMetadata()
	if err != nil {
		return err
	}
	r.currentTerm = term
	r.votedFor = votedFor
	entries, err := r.persister.LoadLogEntries()
	if err != nil {
		return err
	}
	r.walogs = entries
	snapshot, err := r.persister.LoadSnapshot()
	if err != nil {
		return err
	}
	r.snapshot = snapshot
	_ = r.stateMachine.ApplySnapshot(snapshot.Data)
	return nil
}
