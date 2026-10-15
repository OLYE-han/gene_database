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
            grouped = model_data.groupby("branching_factor")[metric].mean().reset_index()
            ax.plot(np.log2(grouped["branching_factor"]), grouped[metric], label=model, marker='o')

        ax.set_title(f"Branching Factor vs {y_labels[metric]}")
        ax.set_xlabel("Branching Factor (Log Scale)")
        ax.set_ylabel(y_labels[metric])
        ax.grid(True)
        ax.legend()

    plt.tight_layout()
    plt.savefig(save_path)

    print(f"Plot saved to {save_path}")

def combine_csv_files(prefix: str, output_file: str = "branching_factor_sensitivity.csv"):
    # Updated header to include model name
    header = "model,input_size,num_procs,sample_size,m,branching_factor,index_time,search_time,recall,activation_rate"
    postfixes = [str(2**i) for i in range(7)]

    with open(output_file, "w") as outfile:
        outfile.write(header + "\n")

        for postfix in postfixes:
            filename = f"{prefix}_pyramid_v2_{postfix}.csv"
            if os.path.exists(filename):
                print(f"File {filename} exists, processing...")
                with open(filename, "r") as infile:
                    for line in infile:
                        line = line.strip()
                        if line and not line.lower().startswith("input_size"):
                            outfile.write(f"pyramid_v2,{line}\n")
            

    print(f"Combined file with model names written to {output_file}")

combine_csv_files("./raw/branching_factor_sensitivity", "branching_factor_sensitivity.csv")
plot_model_metrics("branching_factor_sensitivity.csv", "branching_factor_sensitivity_plot.png")
