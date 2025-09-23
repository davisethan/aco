import sys


def generate_ants(count):
    # write ants to file
    filename = "ants.txt"
    with open(filename, "w") as f:
        for _ in range(count):
            f.write("1\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: ants.py <ant-count>")
        sys.exit(1)
    count = int(sys.argv[1])
    generate_ants(count)
