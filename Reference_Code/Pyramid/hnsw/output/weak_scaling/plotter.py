import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def plot_model_metrics(csv_path: str, save_path: str):
    # Load the CSV.
    df = pd.read_csv(csv_path)

    # Metrics to plot.
    metrics = ["index_time", "search_time", "recall", "activation_rate"]
    y_labels = {
        "index_time": "Index Time (s)",
        "search_time": "Search Time (s)",
        "recall": "Recall",
        "activation_rate": "Activation Rate"
    }

    # Create 2x2 subplot figure.
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    axes = axes.flatten()

    for i, metric in enumerate(metrics):
        ax = axes[i]
        for model in df["model"].unique():
            model_data = df[df["model"] == model]
            grouped = model_data.groupby("num_procs")[metric].mean().reset_index()
            ax.plot(np.log2(grouped["num_procs"]), grouped[metric], marker='o', label=model)

        ax.set_title(f"Number of Processors vs {y_labels[metric]}")
        ax.set_xlabel("Number of Processes (Log Scale)")
        ax.set_ylabel(y_labels[metric])
        ax.grid(True)
        ax.legend()

    plt.tight_layout()
    plt.savefig(save_path)

    print(f"Plot saved to {save_path}")

def combine_csv_files(prefix: str, output_file: str = "weak_scaling.csv"):
    # Updated header to include model name
    header = "model,input_size,num_procs,sample_size,m,branching_factor,index_time,search_time,recall,activation_rate"
    postfixes = [str(2**i) for i in range(10)]
    variants = [("naive_dist", "naive_dist"), ("pyramid_v1", "pyramid_v1"), ("pyramid_v2", "pyramid_v2")]

    with open(output_file, "w") as outfile:
        outfile.write(header + "\n")

        for variant_key, variant_name in variants:
            for postfix in postfixes:
                filename = f"{prefix}_{variant_key}_{postfix}.csv"
                if os.path.exists(filename):
                    print(f"File {filename} exists, processing...")
                    with open(filename, "r") as infile:
                        for line in infile:
                            line = line.strip()
                            if line and not line.lower().startswith("input_size"):
                                outfile.write(f"{variant_name},{line}\n")

    print(f"Combined file with model names written to {output_file}")

combine_csv_files("./raw/weak_scaling", "weak_scaling.csv")
plot_model_metrics("weak_scaling.csv", "weak_scaling_plot.png")

