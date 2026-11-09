"""Off-path cumulative return-budget policy. Native writers never call Python."""
def evaluate(snapshot, settings):
    count=int(snapshot['returns']);total=int(snapshot['remaining_sum'])
    initial=int(snapshot['initial_budget'])
    if count<0 or total<0 or initial<=0 or total>count*initial:
        raise ValueError('Invalid entry return budget counters')
    mean=total/count if count else None
    fraction=mean/initial if count else None
    mode=settings.get('score_mode','conditional_remaining_fraction')
    if mode not in ('conditional_remaining_fraction','expected_remaining_per_query'):
        raise ValueError('Unknown entry admission score mode')
    completed=int(snapshot['completed']) if mode=='expected_remaining_per_query' else snapshot.get('completed')
    if completed is not None and completed<0:
        raise ValueError('Invalid completed entry query count')
    probability=count/completed if completed else None
    expected=total/completed if completed else (0. if count==0 else None)
    threshold=(settings['max_expected_remaining_budget'] if mode=='expected_remaining_per_query'
               else settings['max_mean_remaining_fraction'])
    score=expected if mode=='expected_remaining_per_query' else fraction
    # Native packed return sum/count is coherent, but completed is a separate
    # relaxed atomic. Defer an inconsistent/saturated live snapshot; drained
    # counters are reconciled independently by the benchmark.
    if snapshot.get('saturated',0):reason='saturated_counters'
    elif completed is not None and count>completed:reason='incomplete_snapshot'
    elif count<settings['min_returns']:reason='insufficient_returns'
    elif score is not None and score>threshold:
        reason='excess_expected_return_budget' if mode=='expected_remaining_per_query' else 'early_compute_return'
    else:reason='retained'
    closed=reason in ('excess_expected_return_budget','early_compute_return')
    return dict(snapshot,mean_remaining=mean,mean_remaining_fraction=fraction,
                return_probability=probability,expected_remaining_budget=expected,
                score_mode=mode,score=score,threshold=threshold,close=closed,reason=reason)
