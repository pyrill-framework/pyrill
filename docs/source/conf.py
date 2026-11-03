# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

from datetime import UTC, datetime
from importlib import metadata
from typing import Any

from docutils import nodes
from sphinx import addnodes
from sphinx.application import Sphinx
from sphinx.environment.adapters.toctree import TocTree

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

html_sidebars = {
    "catalog/**": [
        "about.html",
        "searchfield.html",
        "catalog_nav.html",
        "relations.html",
    ],
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


CATALOG_INDEX = "catalog/index"


def page_elements(app: Sphinx, pagename: str) -> nodes.bullet_list | None:
    """Return the objects documented on a page, without their members."""
    toc = TocTree(app.env).get_toc_for(pagename, app.builder)
    element_lists = [
        child
        for page_item in toc.children
        for child in page_item.children
        if isinstance(child, nodes.bullet_list)
    ]
    if not element_lists:
        return None
    elements = element_lists[0]
    for element in elements.children:
        for member_list in list(element.findall(nodes.bullet_list)):
            member_list.parent.remove(member_list)
    return elements


def add_catalog_toc(
    app: Sphinx,
    pagename: str,
    templatename: str,
    context: dict[str, Any],
    doctree: nodes.document | None,
) -> None:
    """Expose the catalog sections, with the current section's elements, to the sidebar."""
    if not pagename.startswith("catalog/"):
        return
    env = app.env
    toc_nodes = list(env.tocs[CATALOG_INDEX].findall(addnodes.toctree))
    if not toc_nodes:
        return
    resolved = TocTree(env).resolve(
        pagename,
        app.builder,
        toc_nodes[0],
        prune=False,
        maxdepth=1,
        titles_only=True,
        collapse=True,
        includehidden=True,
    )
    if resolved is None:
        return
    elements = page_elements(app, pagename) if pagename != CATALOG_INDEX else None
    if elements is not None:
        for item in resolved.findall(nodes.list_item):
            if "current" in item["classes"]:
                item.append(elements)
                break
    context["catalog_toc"] = app.builder.render_partial(resolved)["fragment"]


def setup(app: Sphinx) -> None:
    app.connect("html-page-context", add_catalog_toc)
