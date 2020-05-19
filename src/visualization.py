import matplotlib.pyplot as plt
import geopandas as gpd

def hex_plot(gdf_hex, gdf_data, gdf_boundary, gdf_edges, save_png=False, save_pdf=False,show=False, name=plot,dpi=300,transparent=True):
	fig, ax = plt.subplots(1,1,figsize=(15,15))
	gdf_hex.plot(ax=ax,color='#2b2b2b', alpha=0.95, linewidth=0.1, edgecolor='k', zorder=0)
	gdf_data.plot(ax=ax,column='dist', cmap='magma_r',vmax=1000,zorder=1,legend=True)
	gdf_boundary.boundary.plot(ax=ax,color='#f8f8f8',zorder=2,linestyle='--',linewidth=0.5)
	gdf_edges[(gdf_edges['highway']=='motorway') | (gdf_edges['highway']=='motorway_link')].plot(ax=ax,color='#898989',alpha=0.85,linewidth=2.5,zorder=3)
	gdf_edges[(gdf_edges['highway']=='primary') | (gdf_edges['highway']=='primary_link')].plot(ax=ax,color='#898989',alpha=0.85,linewidth=1.5,zorder=3)
	ax.set_title('Área Metropolitana de Guadalajara\ndistancia a farmacias',fontdict={'fontsize':30})
	ax.axis('off')
	if save_png:
		plt.savefig('../output/figures/{}.png'.format(name),dpi=dpi,transparent=transparent)
	if save_pdf:
		plt.savefig('../output/figures/{}.pdf'.format(name))
	if show:
		plt.show()