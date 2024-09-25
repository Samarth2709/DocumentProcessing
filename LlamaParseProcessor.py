import os
from typing import List, Optional
from pydantic import BaseModel
import openai
from openai import OpenAI
from llama_parse import LlamaParse
import dotenv

dotenv.load_dotenv()

class PageNumbers(BaseModel):
    """
    Pydantic model to define the structure of the page numbers.
    """
    pages: List[int]

class PDFParser:
    """
    A class to parse PDF documents using LlamaParse and save the output as markdown.
    It interacts with OpenAI's gpt-4o-mini model to interpret user-provided page numbers.

    Attributes:
        pdf_path (str): The path to the PDF file.
        openai_api_key (str): The OpenAI API key for accessing the gpt-4o-mini model.
        output_dir (Optional[str]): Directory to save the markdown file. Defaults to the PDF's directory.
    """

    def __init__(self, pdf_path: str, openai_api_key: str, output_dir: Optional[str] = None):
        """
        Initializes the PDFParser with the provided arguments.

        Args:
            pdf_path (str): The path to the PDF file.
            openai_api_key (str): The OpenAI API key.
            output_dir (Optional[str]): Directory to save the markdown file. Defaults to the PDF's directory.
        """
        self.pdf_path = pdf_path
        self.client = OpenAI(api_key=openai_api_key)
        self.openai_api_key = openai_api_key
        self.output_dir = output_dir if output_dir else os.path.dirname(pdf_path)
        openai.api_key = self.openai_api_key

    def get_user_page_input(self) -> Optional[str]:
        """
        Prompts the user to input page numbers in any format.

        Returns: Optional[str]: The user's input for page numbers or None if no input is provided.
        """
        user_input = input(
            "Please enter the page numbers you want to parse (e.g., 'ten through seventeen, 24-53, 20 to 49') or press Enter to parse all pages:\n> "
        ).strip()
        return user_input if user_input else None

    def parse_page_numbers_with_gpt(self, user_input: str) -> List[int]:
        """
        Uses OpenAI's gpt-4o-mini model to parse the user's input into a structured list of page numbers.

        Args: 
            user_input (str): The user's input for page numbers.
        Returns: 
            List[int]: A sorted list of unique page numbers to parse.
        Raises: 
            ValueError: If the AI model fails to return a valid list of integers.
        """
        
        prompt = "Extract all mentioned page numbers from the user's input and return them as a JSON object following the schema {'pages': [list of integers]}."

        try:
            response = self.client.beta.chat.completions.parse(
                model="gpt-4o-mini-2024-07-18",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": user_input},
                    {"role": "system", "content": prompt},
                ],
                response_format=PageNumbers
            )

            # Access the parsed response
            parsed_response: PageNumbers = response.choices[0].message.parsed

            # Remove duplicates and sort
            unique_pages = sorted(list(set(parsed_response.pages)))
            return unique_pages

        except Exception as e:
            print(f"Error parsing page numbers with AI: {e}")
            raise ValueError("Failed to parse page numbers.")

    def get_parsing_instructions(self) -> Optional[str]:
        """
        Prompts the user to input parsing instructions.

        Returns:
            Optional[str]: The user's parsing instructions or None if no input is provided.
        """
        instructions = input(
            "Please enter the parsing instructions or press Enter to skip:\n> "
        ).strip()
        return instructions if instructions else None

    def parse_pdf_to_markdown(self, target_pages: Optional[List[int]], parsing_instructions: Optional[str]):
        """
        Parses the PDF using LlamaParse with the specified pages and instructions, then saves as markdown.

        Args:
            target_pages (Optional[List[int]]): The list of page numbers to parse. If None, all pages are parsed.
            parsing_instructions (Optional[str]): The instructions for parsing.
        """
        parser_options = {
            'result_type': 'markdown',
            'use_vendor_multimodal_model': True,
            'vendor_multimodal_model_name': 'openai-gpt-4o-mini',  # Default model
            # 'vendor_multimodal_model_name': 'openai-gpt-4o',  # Uncomment to use the larger model
            'vendor_multimodal_api_key': self.openai_api_key  # Using the same OpenAI API key
        }

        if target_pages is not None:
            # Convert to zero-based indexing and create a comma-separated string
            zero_based_pages = [str(page - 1) for page in target_pages]  # Assuming user provides 1-based pages
            target_pages_str = ",".join(zero_based_pages)
            parser_options['target_pages'] = target_pages_str

        if parsing_instructions:
            parser_options['parsing_instruction'] = parsing_instructions

        parser = LlamaParse(**parser_options)

        try:
            print("Parsing the PDF document...")
            documents = parser.load_data(self.pdf_path)

            # Combine the parsed text from all documents
            markdown_content = '\n\n'.join([doc.text for doc in documents])

            # Determine the output file path
            pdf_filename = os.path.basename(self.pdf_path)
            markdown_filename = os.path.splitext(pdf_filename)[0] + '.md'
            output_file = os.path.join(self.output_dir, markdown_filename)

            # Save the markdown content to a .md file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(markdown_content)

            print(f'Markdown file saved as {output_file}')

        except Exception as e:
            print(f"Error during parsing: {e}")
            raise

    def run(self):
        """
        Executes the full parsing workflow:
        1. Get user input for page numbers.
        2. If provided, parse the input into structured page numbers using GPT.
        3. Get user input for parsing instructions.
        4. Parse the PDF and save as markdown.
        """
        try:
            user_input = self.get_user_page_input()
            target_pages = None

            if user_input:
                target_pages = self.parse_page_numbers_with_gpt(user_input)
                print(f"Pages to parse: {target_pages}")
            else:
                print("No specific pages provided. All pages will be parsed.")

            parsing_instructions = self.get_parsing_instructions()
            if parsing_instructions:
                print(f"Parsing instructions: {parsing_instructions}")
            else:
                print("No parsing instructions provided.")

            self.parse_pdf_to_markdown(target_pages, parsing_instructions)

        except ValueError as ve:
            print(f"Parsing failed: {ve}")
        except Exception as ex:
            print(f"An unexpected error occurred: {ex}")

if __name__ == "__main__":
    """
    Example usage of the PDFParser class.
    Ensure that you have set your OpenAI API key as an environment variable or replace 'YOUR_OPENAI_API_KEY' with your key.
    """

    # Replace 'YOUR_OPENAI_API_KEY' with your actual OpenAI API key or set it as an environment variable.
    OPENAI_API_KEY = os.getenv('SAMARTH_OPENAI_API_KEY') or 'YOUR_OPENAI_API_KEY'

    if OPENAI_API_KEY == 'YOUR_OPENAI_API_KEY':
        print("Please set your OpenAI API key in the code or as an environment variable 'OPENAI_API_KEY'.")
        exit(1)
    # Replace 'path/to/your/document.pdf' with the actual path to your PDF file.
    pdf_file_path = '/Users/samarthkumbla/Documents/Columbia/ContemporaryCivilizations/Sections - Politics/Book4 1-5.pdf'

    if not os.path.isfile(pdf_file_path):
        print(f"The file {pdf_file_path} does not exist. Please provide a valid PDF file path.")
        exit(1)

    parser = PDFParser(pdf_path=pdf_file_path, openai_api_key=OPENAI_API_KEY)
    parser.run()
