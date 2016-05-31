# -*- coding: utf-8 -*-

import re

from w3lib.html import (
    remove_tags,
    remove_tags_with_content,
)

import nltk


uni2latex={
    u'\xa0': "",
    u'\u2009': "",
    u'\u200a': "",
    u'\u2013': "--",
    u'\u2212': "-",
    u'\u2013': "-",
    u'\uff0b': "+",
    u'\u2217': "*",
    u'\u2032': "'",
    u'\u2019': u"\'",
    '-->': u"$\Rightarrow$",
    '->': u"$\Rightarrow$",
    u'\u2192': u"$\Rightarrow$ ",
    u'\u203e': "$^{-}$",
    u'\xaf': "$^{-}$",
    u'\u2113': u"$\ell$",
    u'\u2213': u"$\mp$",
    u'\xb1': u"$\pm$",
    u'\u2261': u"$\equiv$ ",
    u'\u0305': u"$\bar$",
    u'\u0338': u"$\not$",
    u'\u221a': u"$\sqrt$",
    #
    u'\u03b1': u"$\alpha$",
    u'\u03b2': u"$\beta$",
    u'\u03b3': u"$\gamma$",
    u'\u03b8': u"$\theta$",
    u'\u2061': u"$\Theta$",
    u'\u039b': u"$\Lambda$",
    u'\u03bc': u"$\mu$",
    u'\u03c3': u"$\sigma$",
    u'\u03a3b': u"$\Sigma$",
    u'\u03bd': u"$\nu$",
    u'\u039e': u"$\Xi$",
    u'\u03c0': u"$\pi$",
    u'\u03a5': u"$\\Upsilon$",
    u'\u03d5': u"$\phi$",
    u'\u03c7': u"$\chi$",
    u'\u03c8': u"$\psi$",
    u'\u03a9': u"$\Omega$",
}

def compare_titles(title, splash_title, method="jaccard", required=0.8):
    # Check if the splash title matches with the title in INSPIRE
    # If not, then the metadata in INSPIRE is wrong and error will be logged
    # Have to do this because there are a lot of erronous metadata in INSPIRE

    orig_splash = splash_title
    #if "Study on the Structure of Hypernuclei" in splash_title:
        #import ipdb; ipdb.set_trace()
    splash_title = format_string(splash_title)
    title = format_string(title)

    if method == "jaccard":
        similarity = jaccard(title, splash_title)

    if similarity > required:
        return (similarity, True)
    else:
        return (similarity, False)

def jaccard(a, b):
    """Calculate jaccard similarity between two strings."""
    a = remove_whitespace(a)
    b = remove_whitespace(b)

    a_bigrams = set(nltk.bigrams(a.lower()))
    b_bigrams = set(nltk.bigrams(b.lower()))
    jaccard = len( set(a_bigrams) & set(b_bigrams) )*1.0 / len( set(a_bigrams) | set(b_bigrams) )*1.0

    return jaccard

def format_string(text):
    """Format string to improve distance calculation.

    Prefer LaTeX over html, unicode, or whatever notation.
    Remove special characters.
    Removing latex $ and {} also helps.
    """
    # TODO: add more stuf here
    #if "Double Pomeron Exchange" in text:
        #import ipdb; ipdb.set_trace()
        #text.replace(u"\u03a5", u"$\Upsilon$")
    text = replace_all(text, uni2latex)
    text = text.lower()
    text = convert_html_subscripts_to_latex(text)
    text = remove_tags(text)
    text = text.replace("$", "")
    text = text.replace("{", "")
    text = text.replace("}", "")
    text = text.replace("\\", "")

    return text

def remove_whitespace(text):
    """Remove all whitespace from text."""
    return "".join(text.split())

def convert_html_subscripts_to_latex(text):
    """Convert some HTML tags to latex equivalents."""
    text = re.sub("<sub>(.*?)</sub>", r"$_{\1}$", text)
    text = re.sub("<sup>(.*?)</sup>", r"$^{\1}$", text)
    return text

def replace_all(text, dic):
    """Replace all occurences of certain words in a string."""
    for i, j in dic.iteritems():
        text = text.replace(i, j)
    return text
