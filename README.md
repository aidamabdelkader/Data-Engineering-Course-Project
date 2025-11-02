#  Text Summarization & Scoring with XGBoost and BART

## **Project Overview**
This project demonstrates **text summarization scoring** using both traditional ML and transformer-based approaches. The workflow includes:

- **Text preprocessing**: cleaning, case-folding, expanding contractions, removing stopwords, and generating TF-IDF vectors.  
- **XGBoost model**: predicting summary quality using cosine similarity as a proxy score.  
- **BART model**: generating semantic, context-aware summaries.  
- **Evaluation**: ROUGE metrics for summary quality.  
- **Prompt engineering with LangChain**: context-aware one-shot prompts for improved Q&A and summary generation.  
- **Visualization**: word clouds for exploratory analysis and feature importance interpretation.  

---

## **Objectives**
- Score summaries using **XGBoost** on TF-IDF features.  
- Transform textual data for ML modeling using **cosine similarity**.  
- Generate semantic summaries using **BART**.  
- Apply **ROUGE evaluation** for automated assessment.  
- Enhance summarization and Q&A using **LangChain one-shot prompt**.  
- Explore **word frequencies** and visualize them using **WordCloud**.  

---

## **Key Design Decisions**

### **Text Preprocessing**
- Case-folding: convert all text to lowercase.  
- Remove special characters and multiple spaces.  
- Expand contractions (`don't` → `do not`).  
- Remove English stopwords.  

### **Feature Extraction**
- TF-IDF vectorization with unigrams and bigrams.  
- Vocabulary limited to **5000 features**.  
- Ignored terms that appear in <2 or >90% of documents.

### **Cosine Similarity Summary Score**
- Cosine similarity between document and summary TF-IDF vectors.  
- Generates a **proxy score** for summary quality.

### **XGBoost Regression**
- Hyperparameter tuning via `RandomizedSearchCV`:
  - `max_depth`: [3, 5, 7]  
  - `learning_rate`: [0.01, 0.1, 0.2]  
  - `n_estimators`: [100, 200, 400]  
  - `subsample`: [0.6, 0.7]  
  - `colsample_bytree`: [0.7, 1.0]  
- Early stopping applied to avoid overfitting.  
- Model performance evaluated using **RMSE** between predicted and cosine similarity scores.

### **BART Summarization**
- **Encoder-Decoder Transformer**: Bidirectional encoder (BERT-style) + autoregressive decoder (GPT-style).  
- Tokenization of dialogues and summaries: `max_input_length=512`, `max_target_length=128`.  
- Fine-tuning parameters:
  - Learning rate: 2e-5  
  - Batch size: 4  
  - Epochs: 3  
  - Weight decay: 0.01  
  - Mixed precision (fp16) enabled.  
- Checkpoints saved every epoch; best model loaded at end.  

### **ROUGE Evaluation**
- Compute ROUGE scores (F1) for generated summaries vs reference summaries.  
- Convert token IDs to text before evaluation.  
- Metrics computed automatically using a `compute_metrics` function integrated with the Trainer API.

### **LangChain One-Shot Prompt**
- Context-aware prompts guide the model to generate **concise bullet-point summaries**.  
- Example-driven template ensures consistency in summary style.  
- Uses `HuggingFacePipeline` to wrap BART and integrate with LangChain chains.

---

## **Installation**
```bash
# Clone the repository
git clone <repo-url>
cd <repo-folder>

# Install dependencies
pip install -r requirements.txt

# Optional: For HuggingFace Transformers & Evaluate
pip install transformers evaluate langchain


##  Data Processing


# 1. Clean the text

df['clean_text'] = df['text_column'].apply(case_folding)
df['clean_text'] = df['clean_text'].apply(remove_special_characters)
df['clean_text'] = df['clean_text'].apply(expand_contractions)
df['clean_text'] = df['clean_text'].apply(remove_stopwords)

# 2. Fit TF-IDF vectorizer

tfidf_vectorizer.fit(pd.concat([X_train, Y_train]))
x_train_tfidf = tfidf_vectorizer.transform(X_train)
y_train_tfidf = tfidf_vectorizer.transform(Y_train)

# 3. Compute cosine similarity scores

y_train_sim = np.array([
    cosine_similarity(x_train_tfidf[i], y_train_tfidf[i]).mean()
    for i in range(x_train_tfidf.shape[0])
])

## XGBoost Regression & Hyperparameter Tuning

xgb_model = XGBRegressor(objective='reg:squarederror', random_state=42)
param_dist = { ... }  # see full param list above
random_search = RandomizedSearchCV(xgb_model, param_dist, n_iter=10, scoring='neg_mean_squared_error', cv=3)
random_search.fit(x_train_tfidf, y_train_sim)
best_model = random_search.best_estimator_


## BART Fine Tunning 


from transformers import BartTokenizer, BartForConditionalGeneration, Trainer, TrainingArguments

tokenizer = BartTokenizer.from_pretrained("facebook/bart-base")
model = BartForConditionalGeneration.from_pretrained("facebook/bart-base")

training_args = TrainingArguments(
    output_dir="./bart_finetuned",
    learning_rate=2e-5,
    per_device_train_batch_size=4,
    num_train_epochs=3,
    fp16=True,
    load_best_model_at_end=True
)

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=tokenized_train,
    eval_dataset=tokenized_val,
    tokenizer=tokenizer,
    compute_metrics=compute_metrics
)
trainer.train()


## LangChain One-Shot Prompt Summarization
from langchain.llms import HuggingFacePipeline
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain

pipe = pipeline("summarization", model=model, tokenizer=tokenizer)
llm = HuggingFacePipeline(pipeline=pipe)

prompt = PromptTemplate(
    input_variables=["context", "paragraph"],
    template="""
You are an expert summarizer...
...
Summary:
"""
)

chain = LLMChain(llm=llm, prompt=prompt)
result = chain.run({"context": context, "paragraph": paragraph})
print(result)

from graphviz import Digraph

# Create a directed graph
dot = Digraph(comment='Text Summarization & Scoring Pipeline', format='png')

# Define nodes
dot.node('A', 'Raw Dialogue')
dot.node('B', 'Text Preprocessing\n(cleaning, lowercasing,\nstopwords removal)')
dot.node('C1', 'TF-IDF Vectorizer')
dot.node('C2', 'WordCloud & EDA')
dot.node('D', 'Cosine Similarity\n(proxy summary score)')
dot.node('E', 'XGBoost Model\n(regression on summary scores)')
dot.node('F', 'BART Summarizer\n(Seq2Seq Model)')
dot.node('G', 'LangChain One-Shot Prompt\n(context-aware bullet-point summaries)')
dot.node('H', 'ROUGE Metrics\nEvaluation')
dot.node('I', 'Future: RAG +\nVector Database Integration')

# Connect nodes with arrows
dot.edges(['AB', 'BC1', 'BC2'])
dot.edge('C1', 'D')
dot.edge('D', 'E')
dot.edge('E', 'F')
dot.edge('F', 'G')
dot.edge('G', 'H')
dot.edge('H', 'I')

# Render the graph to a file (PNG)
dot.render('text_summarization_pipeline', view=True)


