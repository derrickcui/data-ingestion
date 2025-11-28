from openai import OpenAI
client = OpenAI(api_key="sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

class LLMProcessor:
    def process(self, data):
        text = data["clean_text"][:4000]

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "user",
                 "content": f"抽取business glossary并输出JSON: {text}"}
            ]
        )

        return { "metadata": resp.choices[0].message.content }
