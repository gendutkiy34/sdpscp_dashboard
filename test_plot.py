import matplotlib.pyplot as plt
import numpy as np

plt.style.use('_mpl-gallery')

# make data
x = ['12 Des','13 Des','14 Des','15 Des','16 Des','17 Des']
y = [110,130,90,200,168,197]

# plot
fig, ax = plt.subplots()

ax.plot(x, y, linewidth=1)
plt.grid(visible=None)

plt.xlabel('xlabel')
plt.ylabel('xlabel')
fig.savefig('MyFigure.png',dpi=1000)
plt.show()