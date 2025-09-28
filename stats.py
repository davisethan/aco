import re
import random
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from collections import defaultdict
from statistics import mean, median, variance
from scipy import stats
from typing import Any, Dict, List, Tuple


class LogParser:
    """
    Parse log files to extract iteration data
    """

    def __init__(self, dirpath: str = None):
        self.fname_re = re.compile(r"^log_(\d+)_(\d+)\.log$")
        self.block_re = re.compile(
            r"Iteration\s+(\d+)\s+New\s+Shortest\s+Path\s+([A-Za-z]+)\s+New\s+Shortest\s+Distance\s+(-?\d+(?:\.\d+)?)",
            re.IGNORECASE | re.DOTALL
        )
        self.logs: Dict[int, Dict[int, List[Tuple[int, str, float]]]] = defaultdict(
            lambda: defaultdict(list))
        self.distances: Dict[int, List[float]] = {}

        if dirpath:
            self.parse_directory(dirpath)
            self.extract_distances()

    def parse_directory(self, dirpath: str) -> Dict[int, Dict[int, List[Tuple[int, str, float]]]]:
        """
        Parse regex filename from a directory
        """
        p = Path(dirpath)
        for child in p.iterdir():
            if not child.is_file():
                continue
            m = self.fname_re.match(child.name)
            if not m:
                continue
            run = int(m.group(1))
            it = int(m.group(2))
            triples = self.parse_log_file(child)
            self.logs[run][it].extend(triples)
        return self.logs

    def parse_log_file(self, filepath: str) -> List[Tuple[int, str, float]]:
        """
        Parse regex blocks from a file
        """
        text = filepath.read_text(encoding="utf-8", errors="replace")
        triples = []
        for m in self.block_re.finditer(text):
            step = int(m.group(1))
            path_name = m.group(2)
            distance = float(m.group(3))
            triples.append((step, path_name, distance))
        return triples

    def extract_distances(self) -> Dict[int, List[float]]:
        """
        Extract shortest distances from parsed log files
        """
        for run, its in sorted(self.logs.items()):
            vals = []
            for _, triples in sorted(its.items()):
                if not triples:
                    continue
                last_dist = triples[-1][2]
                vals.append(last_dist)
            self.distances[run] = vals
        return self.distances


class Boxplotter:
    """
    Boxplotter for visualizing distance distributions
    """

    def plot_boxplots(self, distances: Dict[int, List[float]], outpath: str = "boxplots.png"):
        """
        Plot boxplots of last distances by run
        """
        # Preprocess data
        runs = sorted(distances.keys())
        data = [distances[r] for r in runs]

        # Create boxplot
        plt.figure(figsize=(1 + 1.2 * max(1, len(runs)), 6))
        plt.boxplot(data, positions=range(len(runs)),
                    widths=0.6, patch_artist=True, showfliers=False)
        plt.xticks(range(len(runs)), [str(r) for r in runs])
        plt.xlabel("run")
        plt.ylabel("last distance (per iteration)")
        plt.title("Boxplots of final distances per iteration by run")

        # Overlay datapoints (scatter) with a small random horizontal jitter for visibility
        for i, vals in enumerate(data):
            x = [i + (random.uniform(-0.08, 0.08)) for _ in vals]
            plt.scatter(x, vals, alpha=0.6)

        # Save to disk
        plt.tight_layout()
        plt.savefig(outpath, dpi=150)
        plt.close()


class SummaryStatisticsTabulator:
    """
    Tabulate summary statistics and rank comparisons
    """

    def plot_mean_median_ranks_table(self, distances: Dict[int, List[float]], outpath: str = "rank_table.png"):
        """
        Plot a table comparing mean vs median ranks of runs
        """
        # Compute median and mean
        median_scores = [(run, median(vals))
                         for run, vals in distances.items() if vals]
        mean_scores = [(run, mean(vals))
                       for run, vals in distances.items() if vals]

        # Sort
        median_scores.sort(key=lambda x: x[1])
        mean_scores.sort(key=lambda x: x[1])
        N = max(len(median_scores), len(mean_scores))

        # Prepare rows and columns
        table_data = []
        for i in range(N):
            table_data.append([
                i+1,
                median_scores[i][0] if i < len(median_scores) else "",
                f"{median_scores[i][1]:.3f}" if i < len(median_scores) else "",
                mean_scores[i][0] if i < len(mean_scores) else "",
                f"{mean_scores[i][1]:.3f}" if i < len(mean_scores) else ""
            ])
        col_labels = ["Rank", "Run (Median)", "Median", "Run (Mean)", "Mean"]

        # Create figure
        _, ax = plt.subplots(figsize=(8, 0.3*N))
        ax.axis("off")

        # Make table
        tbl = ax.table(
            cellText=table_data,
            colLabels=col_labels,
            cellLoc="center",
            loc="center",
            bbox=[0, 0, 1, 1]
        )
        tbl.auto_set_font_size(False)
        tbl.set_fontsize(11)

        # Manually widen columns
        col_widths = {
            0: 0.08,  # Rank
            1: 0.22,  # Run (Median)
            2: 0.18,  # Median
            3: 0.22,  # Run (Mean)
            4: 0.18,  # Mean
        }
        for (_, col), cell in tbl.get_celld().items():
            if col in col_widths:
                cell.set_width(col_widths[col])

        # Save
        plt.title("Comparison of Mean vs Median Ranks", fontsize=14, pad=10)
        plt.savefig(outpath, dpi=150,
                    bbox_inches="tight", pad_inches=0.2)
        plt.close()

    def plot_summary_stats_table(self, distances: Dict[int, List[float]], outpath: str = "stats_table.png"):
        """
        Plot a table of summary statistics for each run
        """
        # Compute stats
        rows = []
        for run, values in distances.items():
            rows.append([
                run,
                min(values),
                median(values),
                mean(values),
                variance(values) if len(values) > 1 else 0.0
            ])

        # Column labels
        columns = ["Run", "Min", "Median", "Mean", "Variance"]

        # Create figure
        _, ax = plt.subplots(figsize=(6, 1.8))
        ax.axis("off")

        # Create table
        table = ax.table(
            cellText=[[f"{x:.3f}" if isinstance(
                x, float) else x for x in row] for row in rows],
            colLabels=columns,
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 1.2)

        # Title
        ax.set_title("Run Statistics", fontsize=12, pad=2)

        # Save to file
        plt.savefig(outpath, bbox_inches="tight", dpi=300)
        plt.close()


class DistributionScoreTabulator:
    """
    Fit distributions to data and tabulate metrics
    """

    def __init__(self):
        self.threshold = 7542
        self.dists = {
            "norm": stats.norm,
            "lognorm": stats.lognorm,
            "gamma": stats.gamma,
            "weibull_min": stats.weibull_min
        }
        self.positive_families = ("lognorm", "gamma", "weibull_min")

    def plot_distribution_metrics(self, key: str, results: Dict[str, Any], outpath: str = "results/metrics"):
        """
        Plot a table of distribution fitting metrics for a given parameter set
        """
        # Prepare table data
        table_data = []
        for fam_name in self.dists.keys():
            res = results[fam_name]
            row = [
                f"{res['ll']:.3f}",
                f"{res['AIC']:.3f}",
                f"{res['AICc']:.3f}",
                f"{res['BIC']:.3f}",
                f"{res['p_threshold']:.3f}"
            ]
            table_data.append(row)

        # Prepare table columns and rows
        columns = ["LL", "AIC", "AICc", "BIC", "P<=7542"]
        rows = list(self.dists.keys())

        # Create figure
        fig, ax = plt.subplots(figsize=(6, 1.5))  # width x height
        ax.axis("off")  # hide axes

        # Create table
        table = ax.table(
            cellText=table_data,
            rowLabels=rows,
            colLabels=columns,
            cellLoc="center",
            rowLoc="center",
            loc="center"
        )

        # Style table
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.2, 1.5)
        fig.suptitle(f"Metrics for Parameter Set {key}", fontsize=12, y=0.95)
        plt.tight_layout(rect=[0, 0, 1, 1.1])

        # Save to disk
        fig.savefig(f"{outpath}/param_set_{key}_metrics.png",
                    dpi=300, bbox_inches="tight")
        plt.close(fig)

    def fit_and_score(self, data: List[float], family_name: str) -> Dict[str, Any]:
        """
        Fit a distribution to data and compute information criteria and CDF at threshold
        """
        # Fit distribution by maximum likelihood
        dist = self.dists[family_name]
        data = np.asarray(data)
        n = len(data)
        if family_name in self.positive_families:
            # Often fix loc=0 for positive-only families to stabilize fit
            params = dist.fit(data, floc=0)
            num_fixed = 1
        else:
            params = dist.fit(data)
            num_fixed = 0

        # free parameter count, log likelihood, AIC, AICc, BIC, CDF at threshold
        k = len(params) - num_fixed
        ll = np.sum(dist.logpdf(data, *params))
        aic = 2*k - 2*ll
        aicc = aic + (2*k*(k+1))/(n-k-1) if n-k-1 > 0 else np.inf
        bic = k * np.log(n) - 2*ll
        p_at_threshold = dist.cdf(self.threshold, *params)

        return {
            "params": params,
            "k_free": k,
            "ll": ll,
            "AIC": aic,
            "AICc": aicc,
            "BIC": bic,
            "p_threshold": p_at_threshold,
            "dist": dist
        }


class QQPlotter:
    """
    Create Q-Q plots side-by-side for fitted distributions
    """

    def qq_plots(self, key: str, results: Dict[str, Any], vals: List[float], outpath: str = None):
        """
        Create Q-Q plots for each fitted distribution in a 2x2 grid
        """
        fig, axs = plt.subplots(2, 2, figsize=(10, 8))
        axs = axs.ravel()
        for ax, (name, res) in zip(axs, results.items()):
            self.qq_plot(res["dist"], res["params"], vals, ax=ax, title=name)
        fig.suptitle(f"Parameter set {key} Q-Q plots", fontsize=14)
        plt.tight_layout(rect=[0, 0, 1, 0.96])
        outfile = f"{outpath}/param_set_{key}_qqplots.png"
        fig.savefig(outfile, dpi=300, bbox_inches="tight")
        plt.close(fig)

    def qq_plot(self, dist: str, params: List[float], data: List[float], ax: Any = None, title: str = None):
        """
        Create a Q-Q plot for the given distribution and data
        """
        x = np.sort(np.asarray(data))
        n = len(x)
        probs = (np.arange(1, n+1) - 0.5) / n
        theo_q = dist.ppf(probs, *params)
        if ax is None:
            ax = plt.gca()
        ax.scatter(theo_q, x, s=20)
        mn, mx = np.nanmin(theo_q), np.nanmax(theo_q)
        ax.plot([mn, mx], [mn, mx], "k--", linewidth=1)
        ax.set_xlabel("Theoretical quantiles")
        ax.set_ylabel("Empirical quantiles")
        ax.set_title(title)
        if title is None:
            title = getattr(dist, "name", "dist")
        ax.set_title(title)


class BootstrapTabulator:
    """
    Perform parametric bootstrap on fitted distributions to estimate confidence intervals
    for the CDF at a given threshold
    """

    def __init__(self):
        self.dists = {
            "norm": stats.norm,
            "lognorm": stats.lognorm,
            "gamma": stats.gamma,
            "weibull_min": stats.weibull_min
        }
        self.positive_families = ("lognorm", "gamma", "weibull_min")
        self.threshold = 7542
        self.n_runs_for_any = 10
        self.samples = 10000

    def plot_distribution_confidence_intervals_table(self, key: str, results: Dict[str, Any], vals: List[float], outpath: str = "results/bootstrap"):
        """
        Plot a table of bootstrap confidence intervals for the best fitting distribution
        """
        # Rank by AICc
        ranked = sorted(results.items(), key=lambda kv: kv[1]["AICc"])
        best_family = ranked[0][0]

        # Parametric bootstrap for best family
        probs = self.parametric_bootstrap(vals, best_family)

        # Compute stats
        p_mean = probs.mean()
        p_ci = np.percentile(probs, [2.5, 97.5])
        p_any_samples = 1 - (1 - probs)**self.n_runs_for_any
        p_any_mean = p_any_samples.mean()
        p_any_ci = np.percentile(p_any_samples, [2.5, 97.5])

        # Table data
        table_data = [
            [f"{p_mean:.4f}", f"[{p_ci[0]:.4f}, {p_ci[1]:.4f}]"],
            [f"{p_any_mean:.4f}", f"[{p_any_ci[0]:.4f}, {p_any_ci[1]:.4f}]"]
        ]
        row_labels = ["P(single run <= threshold)",
                      f"P(at least one <= threshold in {self.n_runs_for_any} runs)"]
        col_labels = ["Mean", "95% CI"]

        # Plot table
        fig, ax = plt.subplots(figsize=(8, 1.2))
        ax.axis("off")
        table = ax.table(
            cellText=table_data,
            rowLabels=row_labels,
            colLabels=col_labels,
            loc="center",
            cellLoc="center",
            rowLoc="center"
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 1.5)

        # Title table
        fig.suptitle(f"Bootstrap Confidence Intervals\n(Parameter Set {key}, {best_family})",
                     fontsize=12, y=0.97)
        plt.tight_layout(rect=[0, 0, 1, 1.2])

        # Save to disk
        outfile = f"{outpath}/param_set_{key}_bootstrap_ci.png"
        fig.savefig(outfile, dpi=300, bbox_inches="tight")
        plt.close(fig)

    def parametric_bootstrap(self, data: List[float], family_name: str) -> np.ndarray:
        """
        Perform parametric bootstrap to estimate the distribution of P(X <= threshold)
        """
        data = np.asarray(data)
        n = len(data)
        dist = self.dists[family_name]
        probs = np.empty(self.samples)
        for i in range(self.samples):
            sample = np.random.choice(data, size=n, replace=True)
            if family_name in self.positive_families:
                params = dist.fit(sample, floc=0)
            else:
                params = dist.fit(sample)
            probs[i] = dist.cdf(self.threshold, *params)
        return probs


if __name__ == "__main__":
    # Parse logs
    DIR = "logs"
    distances = LogParser(DIR).distances

    # Plot boxplots
    Boxplotter().plot_boxplots(distances, outpath="results/boxplots/boxplots.png")

    # Plot mean vs median ranks table
    stats_tabulator = SummaryStatisticsTabulator()
    stats_tabulator.plot_mean_median_ranks_table(
        distances, outpath="results/stats/rank_table.png")
    stats_tabulator.plot_summary_stats_table(
        distances, outpath="results/stats/stats_table.png")

    # Fit distributions and plot metrics, Q-Q plots, and bootstrap CIs
    dist_tabulator = DistributionScoreTabulator()
    qq_plotter = QQPlotter()
    bootstrap_tabulator = BootstrapTabulator()
    for key, vals in distances.items():
        # Fit all families
        results = {name: dist_tabulator.fit_and_score(vals, name)
                   for name in dist_tabulator.dists}

        # Fit distribution metrics
        dist_tabulator.plot_distribution_metrics(
            key, results, outpath="results/metrics")

        # Fit distribution Q-Q plots
        qq_plotter.qq_plots(key, results, vals, outpath="results/qqplots")

        # Fit distribution confidence intervals via parametric bootstrap
        bootstrap_tabulator.plot_distribution_confidence_intervals_table(
            key, results, vals, outpath="results/bootstrap")
