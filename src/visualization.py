import matplotlib.pyplot as plt
import geopandas as gpd
from mpl_toolkits.axes_grid1 import make_axes_locatable

def hex_plot(ax, gdf_data, gdf_boundary, gdf_edges, column , title,save_png=False, save_pdf=False,show=False, name='plot',dpi=300,transparent=True, close_figure=True):
	"""
	Plot hexbin geoDataFrames to create the accesibility plots.

	Arguments:
		ax {matplotlib.axes} -- ax to use in the plot
		gdf_data {geopandas.GeoDataFrame} -- geoDataFrame with the data to be plotted
		gdf_boundary {geopandas.GeoDataFrame} -- geoDataFrame with the boundary to use 
		gdf_edges {geopandas.GeoDataFrame} -- geoDataFrame with the edges (streets)
		column {geopandas.GeoDataFrame} -- column to plot from the gdf_data geoDataFrame
		title {str} -- string with the title to use in the plot

	Keyword Arguments:
		save_png {bool} -- save the plot in png or not (default: {False})
		save_pdf {bool} -- save the plot in pdf or not (default: {False})
		show {bool} -- show the plot or not (default: {False})
		name {str} -- name for the plot to be saved if save=True (default: {plot})
		dpi {int} -- resolution to use (default: {300})
		transparent {bool} -- save with transparency or not (default: {True})
	"""
	divider = make_axes_locatable(ax)
	cax = divider.append_axes("bottom", size="5%", pad=0.1)
	gdf_data[gdf_data[column]<=0].plot(ax=ax,color='#2b2b2b', alpha=0.95, linewidth=0.1, edgecolor='k', zorder=0)
	gdf_data[gdf_data[column]>0].plot(ax=ax,column=column, cmap='magma_r',vmax=1000,zorder=1,legend=True,cax=cax,legend_kwds={'label':'Distancia (m)','orientation': "horizontal"})
	gdf_boundary.boundary.plot(ax=ax,color='#f8f8f8',zorder=2,linestyle='--',linewidth=0.5)
	gdf_edges[(gdf_edges['highway']=='motorway') | (gdf_edges['highway']=='motorway_link')].plot(ax=ax,color='#898989',alpha=0.5,linewidth=2.5,zorder=3)
	gdf_edges[(gdf_edges['highway']=='primary') | (gdf_edges['highway']=='primary_link')].plot(ax=ax,color='#898989',alpha=0.5,linewidth=1.5,zorder=3)
	ax.set_title(f'{title}',fontdict={'fontsize':30})
	ax.axis('off')
	if save_png:
		plt.savefig('../output/figures/{}.png'.format(name),dpi=dpi,transparent=transparent)
	if save_pdf:
		plt.savefig('../output/figures/{}.pdf'.format(name))
	if close_figure:
		plt.close()
	if show:
		plt.show()