import argparse
import json
import time
from pathlib import Path

from data.load_yfcc import load_dataset_with_scalars, perform_incremental_load, perform_update, perform_delete

from utils.data_synthesizer import adjust_dimension, adjust_scale, generate_incremental_data
from utils.query_generator import gen_queries_random
from utils.analyzer import Analyzer
from utils.workload_executor import (prepare_config, load_query_from_yaml, 
                                     load_ground_truth, execute_save, execute)
from utils.concurrent import setup_database_c, execute_concurrent, execute_concurrent_hits

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--case", help="The benchmark execution status , data_pre or init or modify_queries or test.", default='init',)
    parser.add_argument("--database", help="the vector database to benchmark", default='milvus',)
    parser.add_argument("--dataset", help="the choice of dataset ", default='YFCC',)
    parser.add_argument("--scale", type=int, default=10_000_000, help="Scale of the test data you want")
    parser.add_argument("--chunk_rows", type=int, default=1_000_000, help="Chunk size for data loading")
    parser.add_argument("--regen_incremental", action="store_true", help="Force regenerate incremental data")
    parser.add_argument("--algorithm", help="the algorithm to be tested", default='hnsw',)
    parser.add_argument("--times", help="the times every single query runs", type=int, default=1,)
    parser.add_argument("--concurrency", help="number of concurrent threads", type=int, default=50)
    parser.add_argument("--ratio", help="the ratio of data to CURD", type=float, default=0.2,)
    parser.add_argument("--in_ratio", help="the ratio of data to insert before creating index", type=float, default=0.2,)
    parser.add_argument("--up_ratio", help="the ratio of data to update", type=float, default=0.2,)
    parser.add_argument("--de_ratio", help="the ratio of data to delete", type=float, default=0.2,)
    parser.add_argument("--phases", nargs="+", default=["P1", "P2", "P3", "P4", "P5", "P6"])
    parser.add_argument("--query_file", default=None)
    parser.add_argument("--ground_truth", default=None)
    parser.add_argument("--output", default="vecbench-results.json")
    args = parser.parse_args()
    return args
 
def setup_database(args, config):
    if args.database == 'pgvector':
        from databases.pgvector import PGVector
        db = PGVector(config, args.database)
    elif args.database == 'milvus':
        from databases.milvus import Milvus
        db = Milvus(config, args.database)
    elif args.database == 'qdrant':
        from databases.qdrant import Qdrant
        db = Qdrant(config, args.database)
    elif args.database == 'weaviate':
        from databases.weaviate import Weaviate
        db = Weaviate(config, args.database)
    elif args.database == 'pyramid':
        from databases.pyramid import Pyramid
        db = Pyramid(config, args.database)
    else:
        raise ValueError("Only support pgvector, milvus, qdrant, weaviate, pyramid now.")
    db.connect()
    print(f"------{db.db_type} connect successfully.")
    return db

def main(args):

    if args.case == 'data_pre':
        adjust_dimension(f"data/{args.dataset}", vec_file="base.10M.u8bin", new_dim=1920)
        adjust_scale(f"data/{args.dataset}", args, target_scale=100_000_000)
        generate_incremental_data(f"data/{args.dataset}", args)
        
    elif args.case == 'init':
        db_config,index_config,schema_config = prepare_config(args)
        db = setup_database(args, db_config)    
        '''construct table or schema'''
        if (args.database == 'qdrant'):
            db.create_table(args.database, schema=schema_config, index = index_config)
        else:
            db.create_table(args.database, schema=schema_config)
        print(f"----{db.db_type} create table successfully.")

        loader_fn = load_dataset_with_scalars(f"./data/{args.dataset}", args, as_list=(db.db_type != 'milvus'))
        for df_chunk, _, _ in loader_fn():
            df_chunk = db.process_data(df_chunk, args.database, schema_config)
            db.insert_data(df_chunk, args.database, schema_config)

        # print(f"Generating query ...")
        # outfile = gen_queries_random(loader_fn, args, 100, f"config/{args.dataset}/E2E_queries.yaml")
        # queries = load_query_from_yaml(args.dataset, outfile)

        queries = load_query_from_yaml(args.dataset, f"config/{args.dataset}/E2E_queries.yaml")
        execute_save(queries, loader_fn ,outpath=f"data/{args.dataset}/ground_truth/E2E_1M_1920.json", save=True)

    elif args.case == 'modify_queries':
        db_config,index_config,schema_config = prepare_config(args)
        db = setup_database(args, db_config)  
        print("Loading original queries ...")
        orig_queries = load_query_from_yaml(args.dataset, f"config/{args.dataset}/E2E_queries_1M_locak_k.yaml")
        print("Preparing loader ...")
        loader_fn = load_dataset_with_scalars(f"./data/{args.dataset}", args, as_list=(db.db_type != 'milvus'))

        print("Running Analyzer ...")
        analyzer = Analyzer(
            loader_fn=loader_fn,
            queries=orig_queries,
            top_k=500,
            compute_filter_rates=True,
            compute_relevances=True,
            tags_denominator="occurrences",
        )

        default_filter_rate = 0.1
        default_relevance_rate = 0.1
        per_query_filter = {
            # "q001": 0.03,
            # "q015": 0.005,
        }
        per_query_relevance = {}
  
        print("Modifying queries ...")
        mode = 'relevance'
        analyzer.run(
            mode=mode,
            default_filter_rate=default_filter_rate,
            default_relevance_rate=default_relevance_rate,
            per_query_filter=per_query_filter,
            per_query_relevance=per_query_relevance,
        )

        out_yaml = f"config/{args.dataset}/E2E_queries_{mode}_modified.yaml"
        analyzer.save(out_yaml)
        print(f"Finished! Modified queries saved to: {out_yaml}")


    elif args.case == 'test':
        db_config,index_config,schema_config = prepare_config(args)
        db = setup_database(args, db_config)
        phases = {phase.upper() for phase in args.phases}
        report = {
            "database": args.database, "dataset": args.dataset, "scale": args.scale,
            "times": args.times, "concurrency": args.concurrency,
            "phases_requested": sorted(phases), "official_vecbench_executor": True,
        }
        def save_report():
            def json_default(value):
                return value.item() if hasattr(value, "item") else str(value)
            Path(args.output).write_text(
                json.dumps(report, indent=2, default=json_default), encoding="utf-8"
            )
        save_report()

        if "P1" in phases:
            print("P1 : Initialization phase is doing.")
            started = time.perf_counter()
            db.create_index(index_config[db.db_type], args)
            if not (args.database == 'qdrant' and db.hnswp):
                db.create_scalar_index(index_config[db.db_type], args)
            report["P1"] = {"T0_seconds": time.perf_counter() - started}
            save_report()
            print("P1 : Initialization phase is finished.")

        queries = ground_truth = None
        if phases.intersection({"P2", "P3"}):
            query_file = args.query_file or f"config/{args.dataset}/E2E_queries.yaml"
            truth_file = args.ground_truth or f"data/{args.dataset}/ground_truth/E2E_192_10M.json"
            queries = load_query_from_yaml(args.dataset, query_file)
            ground_truth = load_ground_truth(truth_file)
        if "P2" in phases:
            print("P2 : Query execution phase is doing.")
            detailed_results, overall_results = execute(
                db, queries, ground_truth, index_config["search_params"], args
            )
            report["P2"] = {"detailed": detailed_results, "overall": overall_results}
            save_report()
            print("P2 : Query execution phase is finished.")

        if "P3" in phases:
            print("P3 : Concurrent phase is doing.")
            db_factory = setup_database_c(args, db_config)
            results = execute_concurrent(
                db_factory, queries, ground_truth, index_config["search_params"], args
            )
            report["P3"] = {"results": results}
            save_report()
            print("P3 : Concurrent phase is finished.")

        if "P4" in phases:
            print("P4 : Incremental load phase is doing.")
            report["P4"] = {"T1_seconds": perform_incremental_load(
                db=db, base_data_dir=f"./data/{args.dataset}", schema=schema_config
            )}
            save_report()
            print("P4 : Incremental load phase is finished.")

        if "P5" in phases:
            print("P5 : Update phase is doing.")
            report["P5"] = {"T2_seconds": perform_update(
                db=db, incremental_dir=f"./data/{args.dataset}/incremental_data",
                schema=schema_config, delta=1
            )}
            save_report()
            print("P5 : Update phase is finished.")

        if "P6" in phases:
            print("P6 : Delete phase is doing.")
            report["P6"] = {"T3_seconds": perform_delete(
                db, incremental_dir=f"./data/{args.dataset}/incremental_data"
            )}
            save_report()
            print("P6 : Delete phase is finished.")

        save_report()
        db.close()

if __name__ == "__main__":
    args = parse_arguments()
    main(args)
