SUMMARY_PROMPT = '''
                In the "Text Content" section, you'll find a document. Please create a summary of this document, ensuring it includes the key information.
                The summary should not exceed {num_words} words and should be presented in a natural and cohesive manner starting immediately with the summary content.                
                
                **Text Content:**
                "{content}"
                '''

