"""Physical replica reservations, separate from the size of a new region.

Missing capacity_policy preserves archived aggregate-alpha semantics. A new
per_region policy applies alpha to a construction, never to all live copies.
All numbers count physical packages, including overlap and draining versions.
"""


def per_region(settings):
    return settings.get('capacity_policy', {}).get('mode') == 'per_region'


def total_limit(settings):
    return settings.get('max_total_replica_bytes',settings['max_region_bytes']*settings['max_replicas'])


def region_limit(settings, source_bytes, tier='small'):
    if tier not in ('small', 'large'):
        raise ValueError('Invalid replica tier')
    alpha = (settings['capacity_policy'][tier+'_alpha'] if per_region(settings)
             else settings['locality']['source_capacity_fraction'])
    return min(settings['max_region_bytes'], int(source_bytes*alpha))


def package_limit(settings, source_bytes, plan):
    base=region_limit(settings,source_bytes,plan.get('replica_tier','small'))
    if (plan.get('region_proof') or {}).get('kind')=='boundary_augmented_v1':
        c=settings.get('boundary_augmentation',{})
        if not c.get('enabled'):raise ValueError('Boundary package allowance disabled')
        base+=c['max_extra_bytes']
    return min(settings['max_region_bytes'],base)


def source_limit(settings, source_bytes):
    if per_region(settings):
        # Zero explicitly disables this optional aggregate source-byte quota.
        return settings['capacity_policy']['max_source_total_bytes'] or total_limit(settings)
    return int(source_bytes*settings['locality']['source_capacity_fraction'])


def target_limit(settings):
    return min(total_limit(settings),
               settings.get('capacity_policy', {}).get('max_target_replica_bytes', total_limit(settings)))


def reservations(controller, exclude=None):
    # Adoption can temporarily appear in pending AND regions. Charge once,
    # using the larger reservation until the pending claim is released.
    rows = {}
    for name in ('regions', 'draining', 'partition_pending', 'build_reservations'):
        for key, value in getattr(controller, name, {}).items():
            if key == exclude:
                continue
            if key not in rows or value.get('bytes', 0) > rows[key].get('bytes', 0):
                rows[key] = value
    return list(rows.values())


def available(controller, source, source_bytes=None, target=None, exclude=None):
    b = controller.b
    rows = reservations(controller, exclude)
    budget = total_limit(b) - sum(r.get('bytes', 0) for r in rows)
    if source_bytes is not None:
        budget = min(budget, source_limit(b, source_bytes) -
                     sum(r.get('bytes', 0) for r in rows if r['source'] == source))
    if target is not None:
        budget = min(budget, target_limit(b) -
                     sum(r.get('bytes', 0) for r in rows if r.get('target') == target))
        load = controller.loads.get(target, {})
        # Free-space telemetry already accounts for written bytes; reserve the
        # full unwritten claims as well (conservative during stream progress).
        pending = {}
        for name in ('partition_pending', 'build_reservations'):
            pending.update(getattr(controller, name, {}))
        unwritten = sum(r['bytes'] for k, r in pending.items()
                        if k != exclude and r['target'] == target)
        budget = min(budget, load.get('free_bytes', 0)-b['reserve_disk_bytes']-unwritten)
    return max(0, budget)


def check_layout(settings, layout):
    if per_region(settings):
        tier = layout.get('replica_tier', 'small')
        if tier not in ('small', 'large'):
            raise ValueError('Invalid replica tier')
        limit = settings['capacity_policy'][tier+'_alpha']
        if tier == 'small':
            limit *= len(layout['target_weights'])
    else:
        limit = settings['locality']['source_capacity_fraction']
    if layout['total_alpha'] > limit+1e-12:
        raise ValueError('Construction exceeds configured region alpha')
