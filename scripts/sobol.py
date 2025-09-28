from SALib.sample import sobol


def main():
    problem = {
        "num_vars": 4,
        "names": ["alpha", "beta", "rho", "num_ants"],
        "bounds": [
            [0.5, 2.0],     # alpha
            [1.0, 5.0],     # beta
            [0.1, 0.9],     # rho
            [50, 250]       # number of ants
        ]
    }
    N = 8
    param_values = sobol.sample(problem, N, calc_second_order=False)

    with open("sobol-test.txt", "w") as f:
        for i, (alpha, beta, rho, num_ants) in enumerate(param_values):
            line = f"{i},{alpha:.3f},{beta:.3f},{rho:.3f},{int(num_ants)}\n"
            f.write(line)


if __name__ == "__main__":
    main()
