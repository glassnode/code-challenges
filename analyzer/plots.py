import matplotlib.pyplot as plt
import numpy as np

def pie(metric, labels, title):
    total = np.sum(metric)
    relative_sizes = [1. * element / total for element in metric]
    #explode = (0.1, 0, 0, 0, 0)  # only "explode" the 2nd slice (i.e. 'Hogs')
    fig1, ax1 = plt.subplots()
    ax1.pie(relative_sizes, labels=labels, autopct='%1.1f%%', shadow=True, startangle=90)

    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    # str_title = "Most transacting addresses since block {}".format(start_point)
    ax1.set(title=title)
    plt.savefig("pie-{}.png".format(title))

def graph_compound(frames, title):
    plt.style.use('ggplot')
    f, g = plt.subplots(figsize=(12, 9))
    for item in frames.keys():
        df = frames[item]
        g = plt.plot(df['tx_date'], df['tx_count'], label=item)
    plt.legend(loc='best')
    plt.title("Daily Tx Count For Week Top-5 Contracts")
    plt.savefig("graph-{}.png".format(title))
