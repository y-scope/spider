from datetime import date

from docutils import nodes
from docutils.parsers.rst import directives
from sphinx.domains.std import ConfigurationValue

# -- Repository information ----------------------------------------------------

# NOTE: When a formal release is cut, update this to the corresponding release branch;
# otherwise, it should stay on "main".
SPIDER_GIT_REF = "main"

# -- Project information -------------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Huntsman"

# NOTE: We don't include a period after "Inc" since the theme adds one already.
copyright = f"2024-{date.today().year} YScope Inc"

# -- General configuration -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
]

# -- MyST extensions -----------------------------------------------------------
# https://myst-parser.readthedocs.io/en/stable/syntax/optional.html
myst_enable_extensions = [
    "attrs_block",
    "colon_fence",
]

myst_heading_anchors = 4

# -- Sphinx autodoc options ----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#configuration

autoclass_content = "class"
autodoc_class_signature = "separated"
autodoc_typehints = "description"

# -- HTML output options -------------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

# NOTE: Source copies are disabled since the theme renders no source links and the copies would
# bypass the `source-read` substitutions below.
html_copy_source = False

html_favicon = "https://docs.yscope.com/_static/favicon.ico"
html_title = project
html_show_copyright = True

html_static_path = ["../src/_static"]

html_theme = "pydata_sphinx_theme"

# -- Theme options -------------------------------------------------------------
# https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/layout.html

html_theme_options = {
    "footer_start": ["copyright"],
    "footer_center": [],
    "footer_end": ["theme-version"],
    "navbar_start": ["navbar-logo"],
    "navbar_end": ["navbar-icon-links", "theme-switcher"],
    "primary_sidebar_end": [],
    "secondary_sidebar_items": ["page-toc", "edit-this-page"],
    "show_prev_next": False,
    "use_edit_page_button": True,
}

# -- Theme source buttons ------------------------------------------------------
# https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/source-buttons.html

html_context = {
    "github_user": "y-scope",
    "github_repo": "spider",
    "github_version": SPIDER_GIT_REF,
    "doc_path": "docs/huntsman/src",
}

# -- Custom directives ---------------------------------------------------------
# https://www.sphinx-doc.org/en/master/extdev/domainapi.html

_CONFVAL_EXTRA_OPTIONS = ("Helm value", "Docker Compose env var")


class _SpiderConfigurationValue(ConfigurationValue):
    option_spec = {
        **ConfigurationValue.option_spec,
        **{name: directives.unchanged_required for name in _CONFVAL_EXTRA_OPTIONS},
    }

    def transform_content(self, content_node):
        super().transform_content(content_node)
        fields = []
        for name in _CONFVAL_EXTRA_OPTIONS:
            if name not in self.options:
                continue
            parsed, msgs = self.parse_inline(self.options[name], lineno=self.lineno)
            fields.append(
                nodes.field("", nodes.field_name("", name), nodes.field_body("", *parsed))
            )
            fields.extend(msgs)
        if not fields:
            return
        if content_node.children and isinstance(content_node.children[0], nodes.field_list):
            content_node.children[0][:0] = fields
        else:
            content_node.insert(0, nodes.field_list("", *fields))


# -- Source transforms ---------------------------------------------------------
# https://www.sphinx-doc.org/en/master/extdev/event_callbacks.html#event-source-read


def _substitute_docs_vars(app, docname, source):
    source[0] = source[0].replace("DOCS_VAR_SPIDER_GIT_REF", SPIDER_GIT_REF)


# -- Theme custom CSS and JS ---------------------------------------------------
# https://pydata-sphinx-theme.readthedocs.io/en/stable/user_guide/static_assets.html


def setup(app):
    app.add_css_file("custom.css")
    app.add_directive_to_domain("std", "confval", _SpiderConfigurationValue, override=True)
    app.connect("source-read", _substitute_docs_vars)
