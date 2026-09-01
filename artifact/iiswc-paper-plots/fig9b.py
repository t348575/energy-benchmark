import matplotlib.pyplot as plt

from common.plot_utils import *
set_standard_font()

fig, ax = plt.subplots()
colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

LABELS=['A', 'B', 'C', 'D', 'E']

y1=[[2.76, 2.19, 2.02], [1.79, 1.78, 1.78], [4.83, 4.83], [4.17, 4.15], [4.77, 4.73]]
y1err=[[0.77, 0.76, 0.67], [0.16, 0.16, 0.16], [0.27, 0.27], [0.28, 0.28], [0.29, 0.29]]

j = 0
for i, y in enumerate(y1):
    if len(y) > 2:
        for k in range(len(y)):
            plt.bar(j + (k - 1) * 0.40, y[k], width=0.35, color=colors[k], linewidth=1, edgecolor='black')
            plt.errorbar(j + (k - 1) * 0.40, y[k], yerr=y1err[i][k], color='black', linewidth=4)
    else:
        for k in range(len(y)):
            plt.bar(j + (k - 0.5) * 0.40, y[k], width=0.35, color=colors[k], linewidth=1, edgecolor='black')
            plt.errorbar(j + (k - 0.5) * 0.40, y[k], yerr=y1err[i][k], color='black', linewidth=4)
    j = j + 2

plt.ylim(0, 6)
plt.xlim(-1, 10)
plt.grid()

ax.set_xticks([x * 2 for x in range(len(LABELS))], [bold(b) for b in LABELS])
ax.set_xlabel(bold("SSD"))
ax.set_ylabel(bold(" Idle Power (W)"))

plt.legend(labels=[bold(b) for b in ['PS0', 'PS1', 'PS2']])
fig.savefig(f'./plots/pst-idle-range.pdf', bbox_inches="tight")
print("see ", f'./plots/pst-idle-range.pdf')
