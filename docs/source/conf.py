# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../..'))

# mock import these packages because readthedocs doesn't have them installed
autodoc_mock_imports = [
    "dateutil",
    "geopandas",
    "matplotlib",
    "matplotlib.cm",
    "matplotlib.colors",
    "matplotlib.pyplot",
    "networkx",
    "numpy",
    "pandas",
    "pyproj",
    "requests",
    "scipy",
    "scipy.spatial",
    "shapely",
    "shapely.geometry",
    "shapely.ops",
    "sklearn",
    "sklearn.neighbors",
	"osmnx"
]


# -- Project information -----------------------------------------------------

project = 'AUP'
copyright = '2020, Luis Natera Orozco'
author = 'Luis G. Natera Orozco'

# The full version, including alpha/beta/rc tags
release = '0.1.0'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon", "rinoh.frontend.sphinx"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'alabaster'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# -- Options for LaTeX output ---------------------------------------------

master_doc = "index"

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #
    'papersize': 'letterpaper',
    # The font size ('10pt', '11pt' or '12pt').
    #
    'pointsize': '10pt',
    # Additional stuff for the LaTeX preamble.
    #
    # 'preamble': '',
    # Latex figure (float) alignment
    #
    # 'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
     (master_doc, "AUP.tex", "AUP Documentation", "Luis G. Natera", "manual"),
 ]
man_pages = [(master_doc, "AUP", "AUP Documentation", [author], 1)]