import matplotlib.pyplot as plt

from common.plot_utils import *
set_standard_font()

colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

LABELS=['Seq Read', 'Rand Read', 'Seq Write', 'Rand Write', 'Rand Mixed']

# Rate limiting 
for ys, ssd in [([[90.648, 90.288], [93.579, 93.817], [90.814, 90.187], [90.490, 90.299], [95.18, 94.62]], 'ssd-a')]:
    fig, ax = plt.subplots()

    j = 0
    for i, y in enumerate(ys):
        plt.bar(j - 0.25, y[0] / 95.18, width=0.45, color=colors[0], linewidth=1, edgecolor='black')
        plt.bar(j + 0.25, y[1] / 95.18, width=0.45, color=colors[1], linewidth=1, edgecolor='black')
        j = j + 2

    plt.ylim(0, 1)
    plt.xlim(-1, 10)
    plt.grid()

    ax.set_xticks([x * 2 - 1 for x in range(len(LABELS))], [bold(b) for b in LABELS])
    plt.xticks(rotation=45)

    ax.set_ylabel(bold(' Efficiency (MiB/j) \% of Max'))

    plt.legend(labels=[bold(b) for b in ['4 KiB', '1 MiB']], loc='lower left')

    path = f'./plots/rate-access-{ssd}'
    save_iiswc_fig(fig, path)

# Normal I/O access
LABELS=['Seq Read', 'Rand Read', 'Seq Write', 'Rand Write', 'Rand Mixed']
for ys, ssd in [\
        ([474.6701683208, 201.81828547142544, 20.819185460908674, 106.11148947164382, 78.44998322224203], 'ssd-a'),\
        ([276.9469478867896, 279.3689845964603, 61.07060948478078, 104.939701089, 104.35013857408065], 'ssd-d')\
        ]:
    fig, ax = plt.subplots()

    j = 0.5
    for i, y in enumerate(ys):
        plt.bar(j-1, y, width=0.5, color=colors[0], linewidth=1, edgecolor='black')
        j = j + 1

    plt.ylim(0, 700)
    ax.set_yticks([200, 400, 600, 700], [bold(yy) for yy in ['200', '400', '600', '700']])
    plt.xlim(-1, 4)
    plt.grid()

    ax.set_xticks([x - 0.5 for x in range(len(LABELS))], [bold(b) for b in LABELS])
    plt.xticks(rotation=45)

    ax.set_ylabel(bold('Efficiency (MiB/J)'))

    path = f'./plots/access-{ssd}'
    save_iiswc_fig(fig, path)

# Double bar with power
LABELS=['Seq Read', 'Rand Read', 'Seq Write', 'Rand Write', 'Rand Mixed']
ssd_list = [\
        ([[474.6701683208, 3.4435643964078273, 0.2992627731921494],\
           [201.81828547142544, 3.2186201206257548, 0.2984801840732567], \
           [20.819185460908674, 2.814820282344984, 0.6733513099662038], \
           [106.11148947164382, 3.121234965696179, 0.5480474799407458], \
           [78.44998322224203, 2.8905787988048783,0.6563245492656298 ]], \
            'ssd-a'),\
        ([[276.94694, 6.596137716166798, 0.43748819117004073],\
           [279.3689845, 6.149515798457635, 0.40338567739578535], \
           [61.07060948478078, 7.1342317363537395, 1.1829539990220128], \
           [104.939701089, 7.091419339127806, 1.0977969108060044], \
           [104.35013857408065, 6.908291236181718, 0.6330654271439454 ]], \
            'ssd-d')\
        ]
for ys, ssd in ssd_list:
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    j = 0.5
    for i, y in enumerate(ys):
        ax1.bar(j - 0.25, y[0], width=0.45, color=colors[0], linewidth=1, edgecolor='black')
        ax2.bar(j + 0.25, y[1], width=0.45, color=colors[1], linewidth=1, edgecolor='black')
        ax2.errorbar(j + 0.25, y[1], yerr=y[2], capsize=3, color='black', ecolor = "black")
        j = j + 2

    ax1.set_ylim(0, 700)
    ax2.set_ylim(0, 10)
    ax1.set_yticks([200, 400, 600, 700], [bold(yy) for yy in ['200', '400', '600', '700']])
    plt.xlim(-1, 10)
    plt.grid()

    ax1.set_xticks([x * 2 + 0.5 for x in range(len(LABELS))], [bold(b) for b in LABELS], rotation=45)

    ax1.set_ylabel(bold('Efficiency (MiB/J)'))
    ax2.set_ylabel(bold('Power (W)'))

    ax1.legend(labels=[bold('Efficiency')], loc='upper left')
    ax2.legend(labels=[bold('Power')], loc='upper right')

    path = f'./plots/access-{ssd}-with-power.pdf'
    save_iiswc_fig(fig, path)

fig, ax1 = plt.subplots()
z = 0
for ys, ssd in ssd_list:

    j = 0.5
    for i, y in enumerate(ys):
        ax1.bar(j - 0.25 + z * 0.5, y[0], width=0.45, color=colors[z], linewidth=1, edgecolor='black') 
        j = j + 2

    ax1.set_ylim(0, 700)
    ax1.set_yticks([200, 400, 600, 700], [bold(yy) for yy in ['200', '400', '600', '700']])
    plt.xlim(-1, 10)
    plt.grid()

    ax1.set_xticks([x * 2 + 0.5 for x in range(len(LABELS))], [bold(b) for b in LABELS], rotation=45)
    z = z + 1

ax1.set_ylabel(bold('Efficiency (MiB/J)'))
ax1.legend(labels=[bold('SSD A'), bold('SSD D')], loc='upper left')
leg = ax1.get_legend()
leg.legendHandles[0].set_facecolor(colors[0])
leg.legendHandles[1].set_facecolor(colors[1])
plt.grid(axis='y', color='gray', linestyle='dashed', linewidth=0.5)

path = f'./plots/access-all-ssds'
save_iiswc_fig(fig, path)
