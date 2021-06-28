import pandas as pd
import seaborn as sns
import matplotlib

dir = 'results'
benchmarks = [
    {
        "title": "first one",
        "filename": "confusion"
    },
    {
        "title": "first one",
        "filename": "confusion_g"
    },
    {
        "title": "first one",
        "filename": "confusion_o"
    },
    {
        "title": "first one",
        "filename": "students"
    },
]


def main():
    for benchmark in benchmarks:
        try:
            df = pd.read_csv(f"{dir}/{benchmark['filename']}")
        except FileNotFoundError:
            continue

        df = df.drop(2)

        plt = sns.barplot(
            data=df,
            y='duration (ms)',
            x='type',
        )
        plt.figure.savefig(
            f"{dir}/{benchmark['filename']}.pdf",
            bbox_inches="tight",
        )


if __name__ == '__main__':
    main()
