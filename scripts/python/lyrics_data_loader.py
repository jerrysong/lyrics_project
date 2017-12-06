import collections
import json
import re

def main():
    with open('lyric.txt') as f:
        while True:
            line = f.readline()
            if not line:
                break

            lyric_id, lyrics_text = load_and_extract(line)
            words = flattern(lyrics_text)
            ret = []
            for word in words:
                normalized_word = normalize_word(word)
                if normalized_word:
                    ret.append(normalized_word)

def get_n_grams(words, n):
    if n == 1:
        return words
    else:
        i = n
        window = collections.deque(words[:n])
        ret = []
        while i != len(words):
            ret.append(''.join(window))
            window.append(words[i])
            window.popleft()
            i += 1
        return ret

# Load and extract the lyric id and lyric content from the given line.
def load_and_extract(line):
    line_json = json.loads(line.strip())
    return line_json['lyricid'], line_json['lyrics']

# Flattern the lyrics text and return a list of words.
def flattern(lyrics_text):
    return re.split('\n| ', lyrics_text)

# Normalize the given word by converting it to lower case only and strip leading
# and trailing non-alphanumeric characters.
def normalize_word(word):
    word = word.lower()

    left = 0
    while left < len(word) and not word[left].isalpha():
        left += 1

    right = len(word)-1
    while right > left and not word[right].isalpha():
        right -= 1

    return word[left:right+1]

