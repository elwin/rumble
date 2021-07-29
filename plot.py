import matplotlib
import pandas as pd
import seaborn as sns
import numpy as np

results_dir = 'results'
output_dir = 'results/generated'
colors = [
    "#E59996",
    "#B66475",
    "#6B7FB3",
    "#56909D",
    "#E5A47B",
    "#F5D57C",
]

benchmarks = [
    # {
    #     "title": "Students: group & order by",
    #     "filename": "students"
    # },
    # {
    #     "title": "Confusion: group & order by",
    #     "filename": "confusion"
    # },
    # {
    #     "title": "Confusion: group by",
    #     "filename": "confusion_g"
    # },
    # {
    #     "title": "Confusion: order by",
    #     "filename": "confusion_o"
    # },
    # {
    #     "title": "Git: group & order by",
    #     "filename": "git"
    # },
    # {
    #     "title": "Git: group by",
    #     "filename": "git_g"
    # },
    # {
    #     "title": "Git: order by",
    #     "filename": "git_o"
    # },
    # {
    #     "title": "Reddit: group & order by",
    #     "filename": "reddit"
    # },
    # {
    #     "title": "Reddit: group by",
    #     "filename": "reddit_g"
    # },
    # {
    #     "title": "Reddit: order by",
    #     "filename": "reddit_o"
    # },
    {
        "title": "Reddit: group & order by",
        "filename": "reddit_2"
    },
    {
        "title": "Reddit: group by",
        "filename": "reddit_2_g"
    },
    {
        "title": "Reddit: order by",
        "filename": "reddit_2_o"
    },
    # {
    #     "title": "Reddit: group & order by",
    #     "filename": "reddit_3"
    # },
    # {
    #     "title": "Reddit: group by",
    #     "filename": "reddit_3_g"
    # },
    # {
    #     "title": "Reddit: order by",
    #     "filename": "reddit_3_o"
    # },
]


def main():
    sns.set_theme(style='whitegrid', palette='Set2', font='Helvetica Neue')
    sns.set_palette(sns.color_palette(colors))

    for benchmark in benchmarks:
        try:
            df = pd.read_csv(f"{results_dir}/{benchmark['filename']}.csv")
        except FileNotFoundError:
            continue

        df['duration (s)'] = df['duration (ms)'] / 1000

        print(benchmark['filename'])
        median = (df
                  .groupby('optimization')
                  .agg(np.median)
                  )
        print(median)

        order = ['default', 'decimalgamma', 'decimalgamma-loose']
        order = [x for x in order if x in df['optimization'].unique().tolist()]

        plt = sns.barplot(
            data=df,
            x='optimization',
            y='duration (s)',
            order=order,
            estimator=np.median,
        )

        median.to_csv(f"{output_dir}/{benchmark['filename']}_median.csv")

        plt.set(xlabel=None, ylabel='median running time (s)')
        plt.set_title(benchmark['title'])
        plt.figure.savefig(f"{output_dir}/{benchmark['filename']}.pdf", bbox_inches="tight")
        plt.figure.savefig(f"{output_dir}/{benchmark['filename']}.png", bbox_inches="tight", dpi=300)
        matplotlib.pyplot.close()


if __name__ == '__main__':
    main()
