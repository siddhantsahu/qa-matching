from xml.etree.ElementTree import ElementTree

import pandas as pd

from ElementTree import XML2DataFrame


def creating_answers_in_desired_format(input_answers_csv, output_answers_csv):
    # df_answers_from_csv = pd.read_csv("./stacksample/Answers.csv", encoding="latin-1")
    df_answers_from_csv = pd.read_csv(input_answers_csv, encoding="latin-1")
    df_answers_from_csv = df_answers_from_csv.rename(columns={'Body': 'Text0'})
    df1_answers_in_desired_format = df_answers_from_csv[['Id', 'Text0']]
    # df1_answers_in_desired_format.to_csv(path_or_buf="./stacksample/answers_reformatted.csv", index=False)
    df1_answers_in_desired_format.to_csv(path_or_buf=output_answers_csv, index=False)

    return df_answers_from_csv


def creating_original_questions_in_desired_format(input_question_csv, output_csv_questions, df_answers_from_csv):
    # df_questions_from_csv = pd.read_csv("./stacksample/Questions.csv", encoding="latin-1")
    df_questions_from_csv = pd.read_csv(input_question_csv, encoding="latin-1")
    df_questions_title_body_joined = df_questions_from_csv
    df_questions_title_body_joined['Text0'] = df_questions_title_body_joined[['Title', 'Body']].apply(
        lambda x: ''.join(x), axis=1)
    df_original_questions = pd.merge(df_answers_from_csv[["Id", "ParentId"]],
                                     df_questions_title_body_joined[["Id", "Text0", "CreationDate"]],
                                     left_on='ParentId', right_on='Id')
    df_original_questions = df_original_questions.drop(['ParentId'], axis=1).rename(columns={'Id_x': 'AnswerId', 'Id_y': 'Id'})
    # df_original_questions.to_csv(path_or_buf="./stacksample/original_questions_reformatted.csv", index=False)
    df_original_questions.to_csv(path_or_buf=output_csv_questions, index=False)

    return df_questions_title_body_joined


def creating_duplicate_questions_in_desired_format(input_postlinks_csv, output_duplicate_questions_csv, df_answers_from_csv, questions_title_body_joined):
    # df_postlinks = pd.read_csv('./postlinks.csv', encoding='latin-1')
    df_postlinks = pd.read_csv(input_postlinks_csv, encoding='latin-1')

    df_postlinks_filter_duplicates = df_postlinks[df_postlinks.LinkTypeId == 3]

    # duplicates_result = pd.merge(df_answers_from_csv, df_postlinks_filter_duplicates, left_on='ParentId', right_on='PostId')

    '''dups_ques_from_answers = pd.merge(df_answers_from_csv[["ParentId"]], questions_title_body_joined[["Id", "Text0"]],
                                      left_on="ParentId", right_on="Id")'''

    duplicates_result_answers_test = pd.merge(df_answers_from_csv[["Id", "ParentId"]],
                                              df_postlinks_filter_duplicates[["PostId", "RelatedPostId"]],
                                              left_on='ParentId', right_on='RelatedPostId')

    duplicates_result_answers_test = duplicates_result_answers_test.drop(['RelatedPostId'], axis=1).rename(columns={'Id': 'AnswerId', 'ParentId': 'QuestionId'})

    df_dupe_questions = pd.merge(duplicates_result_answers_test[['AnswerId', 'QuestionId', 'PostId']],
                                 questions_title_body_joined[['Id', 'Text0']], left_on='QuestionId', right_on='Id')

    df_duplicate_question = pd.merge(questions_title_body_joined, duplicates_result_answers_test, left_on='Id',right_on='PostId')

    df_duplicate_question = df_duplicate_question.rename(columns={'Text0_x': 'Text0'})

    df_duplicate_questions_azure_format = df_duplicate_question[['QuestionId', 'AnswerId', 'Text0', 'CreationDate']]

    df_duplicate_questions_azure_format = df_duplicate_questions_azure_format.rename(columns={'QuestionId': 'Id'})

    '''df_duplicate_questions_azure_format.to_csv(path_or_buf="./stacksample/duplicates_questions_reformatted.csv",
                                               index=False)'''

    df_duplicate_questions_azure_format.to_csv(path_or_buf=output_duplicate_questions_csv,
                                               index=False)


def creating_postlinks_csv(postlinks_xml_file_path, output_postlinks_csv_path):
    '''with open('./PostLinks.xml', 'r') as f:
        xml_data = f.read()'''
    with open(postlinks_xml_file_path, 'r') as f:
        xml_data = f.read()
    xml2df = XML2DataFrame(xml_data)
    xml_dataframe = xml2df.process_data()
    # xml_dataframe.to_csv('./postlinks.csv', index=False)
    xml_dataframe.to_csv(output_postlinks_csv_path, index=False)

print("INFO: Original Answers csv creation initiated")
original_answers_csv = creating_answers_in_desired_format("../stacksample/Answers.csv", "../stacksample/Answers_reformatted.csv")
print("INFO: Original Answers csv created")

print("INFO: Original Questions csv creation initiated")
questions_title_body_joined = creating_original_questions_in_desired_format("../stacksample/Questions.csv", "../stacksample/Questions_reformatted.csv",original_answers_csv)
print("INFO: Original Questions csv created")

print("INFO: Postlinks csv creation initiated")
creating_postlinks_csv("../PostLinks.xml", "../stacksample/PostLinks.csv")
print("INFO: Postlinks csv created")

print("INFO: Duplicate Questions csv creation initiated")
creating_duplicate_questions_in_desired_format("../stacksample/PostLinks.csv", "../stacksample/duplicate_questions_reformatted.csv", original_answers_csv, questions_title_body_joined)
print("INFO: Duplicate Questions csv created")


