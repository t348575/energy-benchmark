import matplotlib.pyplot as plt
import os

def save_iiswc_fig(fig, path):
    # First create dirs if they don't exist
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Then save
    for f_format in ["pdf", "png"]:
        fig.savefig(f"{path}.{f_format}", bbox_inches="tight") 
        print("see ", f"{path}.{f_format}")
    # Side-effect, but we do not view live anyway
    plt.close(fig)

def set_font(size):
    text_font_size = size
    marker_font_size = size
    label_font_size = size
    axes_font_size = size

    plt.rc('pdf', use14corefonts=True, fonttype=42)
    plt.rc('ps', useafm=True)
    plt.rc('font', size=text_font_size, weight="bold", family='serif', serif='cm10')
    plt.rc('axes', labelsize=axes_font_size,labelweight="bold")    
    plt.rc('xtick', labelsize=label_font_size)    
    plt.rc('ytick', labelsize=label_font_size)    
    plt.rc('legend', fontsize=label_font_size)  

    plt.rcParams['text.usetex'] = True    
    plt.rcParams['text.latex.preamble'] = r'\boldmath'

def set_standard_font():
    set_font(21)

def bold(text):
    return r'\textbf{' + text + r'}'

# Color rules (based on https://personal.sron.nl/~pault/)
GREEN = "#117733"
TEAL  = "#44AA99"
CYAN = "#88CCEE"
OLIVE = "#999933"
SAND = "#DDCC77"
ROSE   = "#CC6677"
BLUE = "#88CCEE"
MAGENTA = "#AA4499"
GREY = GRAY = "#DDDDDD"

