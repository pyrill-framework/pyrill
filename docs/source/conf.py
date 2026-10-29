# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

from datetime import UTC, datetime
from importlib import metadata

# -- Project information -----------------------------------------------------

project = "PyRill"
copyright = f"{datetime.now(UTC).year}, alfred82santa"
author = "alfred82santa"

# The full version, including alpha/beta/rc tags
release = metadata.version("pyrill")

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.autodoc"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "alabaster"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_logo = "_static/brand/logo.svg"
html_favicon = "_static/brand/favicon.ico"
html_theme_options = {
    "logo_name": True,
}

autodoc_default_options = {
    "member-order": "bysource",
    "undoc-members": True,
    "members": True,
    "show-inheritance": True,
}

autoclass_content = "init"
autodoc_class_signature = "separated"
autodoc_typehints = "both"
autodoc_preserve_defaults = True
autodoc_inherit_docstrings = True
