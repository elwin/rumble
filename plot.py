import matplotlib
import pandas as pd
import seaborn as sns

results_dir = 'results'
benchmarks = [
    {
        "title": "Students: group & order by",
        "filename": "students"
    },
    {
        "title": "Confusion: group & order by",
        "filename": "confusion"
    },
    {
        "title": "Confusion: group by",
        "filename": "confusion_g"
    },
    {
        "title": "Confusion: order by",
        "filename": "confusion_o"
    },
    {
        "title": "Git: group & order by",
        "filename": "git"
    },
    {
        "title": "Git: group by",
        "filename": "git_g"
    },
    {
        "title": "Git: order by",
        "filename": "git_o"
    },
]

sns.set_theme(style='darkgrid', palette='Set2', font='DejaVu Sans')  # font='CMU Serif'


def main():
    for benchmark in benchmarks:
        try:
            df = pd.read_csv(f"{results_dir}/{benchmark['filename']}.csv")
        except FileNotFoundError:
            continue

        df['duration (s)'] = df['duration (ms)'] / 1000
        # df = df.drop(2)

        plt = sns.barplot(
            data=df,
            y='duration (s)',
            x='optimization',
        )
        plt.set_title(benchmark['title'])
        plt.figure.savefig(
            f"{results_dir}/{benchmark['filename']}.pdf",
            bbox_inches="tight",
        )
        matplotlib.pyplot.close()


if __name__ == '__main__':
    main()
