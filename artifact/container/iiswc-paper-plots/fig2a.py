import matplotlib.pyplot as plt

from common.plot_utils import *
set_standard_font()

fig, ax = plt.subplots()
colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA]

LABELS=['A', 'B', 'C', 'D', 'E']
y1=[4.52, 5.88, 9.39, 7.3, 13.6]
y2=[1.55, 0.77, 3.7, 3.58, 4.24]

y1 = [x1 - x2 for x1,x2 in zip(y1,y2)]

plt.bar([bold(x) for x in LABELS], y1, bottom=y2, color=colors[1:], linewidth=1, edgecolor='black')

plt.ylim(0, 20)
plt.xlim(-1, 5)
plt.grid()

ax.set_xlabel(bold("SSD model"))
ax.set_ylabel(bold("Power (W)"))

path='./plots/ssd-power-range.pdf'
fig.savefig(path, bbox_inches="tight")
print("see ", path)
