#!/usr/bin/env python3

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import json


__author__ = ""
__email__ = ""

# Depending on your implementation,
# this data may or may not be useful.
# Many students last year found it redundant.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """

    # YOUR CODE GOES BELOW:
    parsed_text = text

    parsed_text = parsed_text.replace("\n", " ")
    parsed_text = parsed_text.replace("\t", " ")
    parsed_text = parsed_text.replace("[", "")
    parsed_text = parsed_text.replace("]", "")
    parsed_text = re.sub(r'(https?://|www\.)\S+', '', parsed_text)
    parsed_text = parsed_text.split()
    parsed_text = " ".join(parsed_text)

    parsed_text = list(parsed_text)
    external_punctuation = [',', '.', '!', '?', ';', ':']
    for i in range(len(parsed_text)):
        if parsed_text[i] in external_punctuation:
            if i != len(parsed_text)-1 and parsed_text[i+1] != ' ':
                parsed_text[i] = parsed_text[i] + " "
            if i != 0 and parsed_text[i-1] != ' ':
                parsed_text[i] = " " + parsed_text[i]
    parsed_text = ''.join(parsed_text)

    parsed_text = parsed_text.split()
    var_l = True
    j = 0
    if len(parsed_text) == 0:
        var_l = False
    while var_l:
        if parsed_text[j] not in external_punctuation:
            l = len(parsed_text[j])
            index = -1
            for i in range(l):
                if parsed_text[j][i].isalpha() or parsed_text[j][i].isdigit():
                    index = i
                    break

            if index == -1:
                parsed_text.remove(parsed_text[j])
                if j < len(parsed_text):
                    var_l = True
                else:
                    var_l = False
                continue
            else:
                parsed_text[j] = parsed_text[j][index:l]

            l = len(parsed_text[j])

            for i in range(l-1, -1, -1):
                if parsed_text[j][i].isalpha() or parsed_text[j][i].isdigit():
                    index = i
                    break

            parsed_text[j] = parsed_text[j][0:index+1]
        j +=1
        if j < len(parsed_text):
            var_l = True
        else:
            var_l = False

    parsed_text = " ".join(parsed_text)

    parsed_text = parsed_text.lower()

    tokens = parsed_text.split()
    length = len(tokens)

    furuya = []
    for i in range(length):
        if tokens[i] in external_punctuation:
            continue
        else:
            furuya.append(tokens[i])
    unigrams = ' '.join(furuya)

    amuro = []
    for i in range(length-1):
        if tokens[i] in external_punctuation or tokens[i+1] in external_punctuation:
            continue
        else:
            amuro.append(tokens[i]+"_"+tokens[i+1])
    bigrams = ' '.join(amuro)

    bourbon = []
    for i in range(length-2):
        if tokens[i] in external_punctuation or tokens[i+1] in external_punctuation or tokens[i+2] in external_punctuation:
            continue
        else:
            bourbon.append(tokens[i]+"_"+tokens[i+1]+"_"+tokens[i+2])
    trigrams = ' '.join(bourbon)


#([^a-zA-Z0-9]*)([a-zA-Z0-9]\S*[a-zA-Z0-9])
#a (*a)?



    return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.

    # We are "requiring" your write a main function so you can
    # debug your code. It will not be graded.

    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    args = parser.parse_args()

    with open(args.filename, "r", encoding="utf8") as file:
        for line in file:
            d = json.loads(line)
            text = d["body"]
            print(sanitize(text))

