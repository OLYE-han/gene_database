"""Build certified portal partitions from history, without changing live routes."""
import argparse
import json
from pathlib import Path
import yaml
from dhdgs.config import StrictLoader
from dhdgs.index import Shard
from dhdgs.replica_layout import build_partitioned_regions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--history', type=Path, required=True, help='hot_ids, weights and training_query_ids JSON')
    parser.add_argument('--shard', type=Path, required=True)
    parser.add_argument('--source', type=int, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    cfg = yaml.load(args.config.read_text(), Loader=StrictLoader)['load_balance']
    history = json.loads(args.history.read_text())
    if str(args.source) in cfg['partitioned_regions']['target_weights']:
        raise ValueError('Replica target must differ from source')
    args.output.mkdir(parents=True, exist_ok=False)
    emitted = []
    def save_plan(plan):
        plan.update(source=args.source, training_query_ids=history['training_query_ids'], min_score=-1e300, max_score=1e300)
        name = 'target_' + str(plan['target']) + '.json'
        temporary = args.output / (name + '.tmp')
        temporary.write_text(json.dumps(plan))
        temporary.replace(args.output / name)
        emitted.append(name)
    shard = Shard(args.shard, args.source)
    try:
        plans, report = build_partitioned_regions(shard, history['hot_ids'], history['weights'], cfg, cfg['partitioned_regions'], on_plan=save_plan)
    except BaseException as exc:
        (args.output / 'FAILED.json').write_text(json.dumps(dict(error=str(exc), completed_plan_files=emitted)))
        raise
    finally:
        shard.close()
    files = emitted
    (args.output / 'layout.json').write_text(json.dumps(dict(report, plan_files=files), indent=2))


if __name__ == '__main__':
    main()
