import re
import ast
import csv


def n_gram(text_file, gram=1):
    total_dict = {}
    with open(text_file, 'r') as file:
        for x in range(1, gram + 1):
            n_gram_dict = {}
            tracker = 0
            for line in file:
                text = re.sub(r'[^\w\s]', '', line).lower().split()
                for index, word in enumerate(text):
                    if index + x > len(text):
                        break
                    words_upto = text[index:index + x]
                    words_joined = " ".join(words_upto)
                    n_gram_dict[words_joined] = n_gram_dict.get(words_joined, 0) + 1
                    if n_gram_dict[words_joined] == 1:
                        tracker += 1
            n_gram_dict['Total_CW_Flag'] = tracker
            key_label = str(x) + "-gram"
            total_dict[key_label] = n_gram_dict
            file.seek(0)
    return total_dict


def edit_second_gram(ngram, gramString):
    for x in ngram[gramString]:
        ngram[gramString][x] = (ngram[gramString][x], x.split()[0])
    return ngram


def writeFile(ngram_dict):
    for key in ngram_dict:
        string_name = str(key) + ".txt"
        with open(string_name, mode='w') as file:
            for subkey in ngram_dict[key]:
                gram_writer = csv.writer(file, delimiter='\t', quotechar='"')
                gram_writer.writerow([subkey, ngram_dict[key][subkey]])


def openFile(fileName):
    word_list, count_list = [], []
    with open(fileName, 'r') as file:
        file_opener = csv.reader(file, delimiter='\t')
        new_dict = {}
        for row in file_opener:
            # print(row)
            word_list.append(row[0])
            count_list.append(row[1])
            new_dict[row[0]] = ast.literal_eval(row[1])
    return word_list, count_list, new_dict


def prediction_dict(newDict):
    nextWordDict = {}
    fwl = []
    ll = []
    for pair in newDict:
        firstWord = newDict[pair][1]

        if nextWordDict.get(firstWord) == None:
            nextWordDict[firstWord] = []
            # print((newDict[pair][0], pair.split()[1]))
            if len(pair.split()) == 1:
                word = pair.split()[0]
            else:
                word = pair.split()[1]
            nextWordDict[firstWord].append((word, newDict[pair][0]))

        else:
            nextWordDict[firstWord].append((pair.split()[1], newDict[pair][0]))
        nextWordDict[firstWord].sort(key=lambda x: x[1], reverse=True)

    for key in nextWordDict:
        fwl.append(key)
        ll.append(nextWordDict[key])
    return nextWordDict, fwl, ll


def nextBestWord(fwl, inputWord, table):

    next_words_list = table.select_specific_data(inputWord, fwl)
    if next_words_list == None:
        return "No best word found"
    #next_words_dict = dict(next_words_list)
    nextWord = next_words_list[0][0]
    output = "Next best word after '{0}' is '{1}'".format(inputWord, nextWord)
    return output

