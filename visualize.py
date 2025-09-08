import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

# 数据集
results_data = {
    "DBLP": {
        "Add Vertex": {"NO Transaction": 31.66, "Transaction": 2355.34},
        "Add Edge": {"NO Transaction": 40.37, "Transaction": 1430.97},
        "Delete Edge": {"NO Transaction": 19.72, "Transaction": 1199.21}
    },
    "COM-LJ": {
        "Add Vertex": {"NO Transaction": 23.87, "Transaction": 2788.31},
        "Add Edge": {"NO Transaction": 87.14, "Transaction": 2642.62},
        "Delete Edge": {"NO Transaction": 11.74, "Transaction": 1779.72}
    },
    "Cit-Patents": {
        "Add Vertex": {"NO Transaction": 18.87, "Transaction": 2606.59},
        "Add Edge": {"NO Transaction": 34.59, "Transaction": 1802.87},
        "Delete Edge": {"NO Transaction": 11.36, "Transaction": 1393.62}
    }
}

# 将所有数据整合到一个DataFrame中，并增加一个'Dataset'列来区分来源
all_dfs = []
for dataset_name, data in results_data.items():
    df = pd.DataFrame(data).T
    df = df.reset_index().rename(columns={'index': 'Operation'})
    df_melted = df.melt(id_vars='Operation', var_name='Transaction Type', value_name='Time (ms)')
    df_melted['Dataset'] = dataset_name
    all_dfs.append(df_melted)

combined_df = pd.concat(all_dfs, ignore_index=True)

# --- 使用 Seaborn 的 FacetGrid (catplot) 创建子图 ---

# 设置绘图风格
sns.set_theme(style="whitegrid")

# 使用 catplot 创建一个基于数据集的分面网格图
g = sns.catplot(
    data=combined_df,
    x='Operation',
    y='Time (ms)',
    hue='Transaction Type',
    col='Dataset', # 关键：根据'Dataset'列来创建子图
    kind='bar',
    palette='muted',
    height=6, # 子图的高度
    aspect=0.9 # 子图的宽高比
)

# 将 Y 轴设置为对数刻度
g.set(yscale="log")

# 调整 Y 轴标签，防止科学计数法显示
for ax in g.axes.flat:
    ax.yaxis.set_major_formatter(plt.ScalarFormatter())
    # 在每个条形上添加数值标签
    for p in ax.patches:
        # 获取条形的高度（即Y值）
        height = p.get_height()
        # 只有当高度大于0时才添加标签（防止log(0)错误）
        if height > 0:
            ax.annotate(f'{height:.2f}', # 格式化标签文本
                       (p.get_x() + p.get_width() / 2., height), # 标签的位置
                       ha = 'center', va = 'center',
                       xytext = (0, 9),
                       textcoords = 'offset points')

# 设置主标题和坐标轴标签
g.fig.suptitle('Performance Comparison Across Datasets', y=1.03, fontsize=16) # y > 1.0 以免和子图标题重叠
g.set_axis_labels("Operation", "Time (ms) - Log Scale")
g.set_titles("{col_name}") # 设置每个子图的标题

# 自动调整图例位置
sns.move_legend(g, "upper right", bbox_to_anchor=(.95, .95))

# 调整整体布局
plt.tight_layout(rect=[0, 0, 1, 0.97]) # rect 调整边界，为大标题留出空间

# 显示图表
plt.show()

# 如果需要将图表保存为文件，可以取消下面的注释
g.savefig("performance_comparison_log_scale.png", dpi=300)
print("图表已保存为 performance_comparison_log_scale.png")