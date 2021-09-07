import initialize as iz
import tableClass as tc

testing_file_a = '/var/text_data/combined_2020_9_30.txt'
ngram_dict = iz.n_gram(testing_file_a, gram=2)
ngram_dict_edit = iz.edit_second_gram(ngram_dict, "2-gram")

iz.writeFile(ngram_dict_edit)
word_list, count_list, newDict = iz.openFile("2-gram.txt")
nextWord, fwl, ll = iz.prediction_dict(newDict)


if __name__ == "__main__":
    #print(ll[5])
    #print("testing")
    predictTable = tc.TXTCassandra()
    predictTable.createsession()
    predictTable.createkeyspace("nextword")
    predictTable.create_table_next("nextword")
    predictTable.insert_data_next(fwl, ll)
    #predictTable.update_data_next('covid18', 'cases', 20)
    #predictTable.update_data_next('covid19', 'cases', 20)

    #next_words = predictTable.select_specific_data('even', fwl)
    output = iz.nextBestWord(fwl, 'when', predictTable)

    print(output)
