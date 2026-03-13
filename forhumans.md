Initial prompt 
py scripts\llm_prompt.py "Explain vLLM in one paragraph."

PART 1: Metadata ve Request olduğunda SQL oluşturma

$prompt = @"
You are a senior SQL engineer. Your task is to generate a single SQL query based strictly on the provided metadata and requirements.

Rules:
- Output ONLY the SQL query. No explanations, no markdown, no comments.
- Use only tables, columns, and joins that exist in the metadata.
- Prefer explicit JOINs; never use SELECT *.
- Only include columns needed to satisfy the request.
- Apply filters as early as possible (WHERE).
- If aggregating, include proper GROUP BY.
- If a field is ambiguous, choose the most reasonable based on metadata; do not ask questions.
- If the request cannot be satisfied with the metadata, output exactly: INVALID_REQUEST

Optimization priorities:
- Minimize scanned data (filters, selective columns).
- Use sargable predicates (no functions on indexed columns if avoidable).
- Avoid unnecessary subqueries; use CTEs only if it improves clarity or reuse.
- Ensure join keys are indexed when possible (use PK/FK).

Metadata:
- Dialect: oracle sql
- Tables:
  - customers(customer_id int, name text, email text, created_at date), PK(customer_id)
  - orders(order_id int, customer_id int, order_date date, total_amount numeric), PK(order_id), FK(customer_id -> customers.customer_id)
  - order_items(order_item_id int, order_id int, product_id int, quantity int, unit_price numeric), PK(order_item_id), FK(order_id -> orders.order_id)
  - products(product_id int, name text, category text), PK(product_id)
- Indexes: orders(customer_id, order_date), order_items(order_id), products(category)

Request:
List each customer's name and total spend in 2024, only for customers who spent more than 1000, sorted by total spend descending.
"@
py scripts\llm_prompt.py $prompt


Request kısmındaki kelimelere göre veriden ihtiyacımız olan metadata çekilecek ve promptun metadata kısmı oluşturulacak.





PART 2: Requestten ihtiyacımız olan metadata bilgisi oluşturma

$prompt = @"
You are a senior data modeler. Your task is to read the request and identify the minimal set of tables, columns, joins, and operations needed to answer it using the provided metadata.

Rules:
- Output ONLY in the specified format. No explanations, no markdown, no comments.
- Include only columns required to satisfy the request.
- For each column, list its role(s): select, filter, join, group, aggregate, sort.
- Prefer PK/FK join keys when available.
- If a derived field is required, list it under DERIVED with its formula.
- If the request cannot be satisfied with the metadata, output exactly: INVALID_REQUEST

Output format:
TABLES:
- <table>: <column> [role, role], <column> [role]
JOINS:
- <table>.<column> = <table>.<column>
FILTERS:
- <table>.<column> <predicate>
GROUP BY:
- <table>.<column>, <table>.<column>
HAVING:
- <aggregate predicate>
ORDER BY:
- <expression> ASC|DESC
DERIVED:
- <alias> = <expression>

Metadata:
- Dialect: postgres
- Tables:
  - customers(customer_id int, name text, email text, created_at date), PK(customer_id)
  - orders(order_id int, customer_id int, order_date date, total_amount numeric), PK(order_id), FK(customer_id -> customers.customer_id)
  - order_items(order_item_id int, order_id int, product_id int, quantity int, unit_price numeric), PK(order_item_id), FK(order_id -> orders.order_id)
  - products(product_id int, name text, category text), PK(product_id)
- Indexes: orders(customer_id, order_date), order_items(order_id), products(category)

Request:
List each customer's name and total spend in 2024, only for customers who spent more than 1000, sorted by total spend descending.
"@
py scripts\llm_prompt.py $prompt


PART 2.5: Requestten ihtiyacimiz olan tablolarin listesi

$prompt = @"
You are a senior data modeler. Your task is to read the request and return the set of metadata needed to answer it using the provided metadata. We will use vector search to find relevant metadata, so include all potentially useful items.

Rules:
- Output ONLY in the specified format. No explanations, no markdown, no comments.
- Use only tables that exist in the metadata.
- Include any table that could plausibly contribute to select, filter, join, group, aggregate, or sort (favor recall over precision).
- Include columns that might be useful, including candidate keys, date fields, categorical fields, and amounts even if not explicitly requested.
- Include relevant PK/FK relationships and helpful indexes if they could aid filtering or joining.
- Prefer join paths using PK/FK relationships when available.
- If the request cannot be satisfied with the metadata, output exactly: INVALID_REQUEST

Output format:
TABLES:
- <table>
COLUMNS:
- <table>: <column>, <column>, ...
JOINS:
- <table>.<column> = <table>.<column>
INDEXES:
- <table>(<column>, <column>)

Metadata:
- Dialect: oracle sql
- Tables:
  - customers(customer_id int, name text, email text, created_at date), PK(customer_id)
  - orders(order_id int, customer_id int, order_date date, total_amount numeric), PK(order_id), FK(customer_id -> customers.customer_id)
  - order_items(order_item_id int, order_id int, product_id int, quantity int, unit_price numeric), PK(order_item_id), FK(order_id -> orders.order_id)
  - products(product_id int, name text, category text), PK(product_id)
- Indexes: orders(customer_id, order_date), order_items(order_id), products(category)

Request:
List each customer's name and total spend in 2024, only for customers who spent more than 1000, sorted by total spend descending.
"@
py scripts\llm_prompt.py $prompt



PART 3: ihtiyacımız olanlar çıktısıyla veritabanındaki asıl metadatayı 




plan:
Yazılı prompt
prompta bakarak metadatadan hangi tablolara ihtiyacı olabileceğini bulsun
Prompta bakarak metadatadan yazabileceğin 3 sorgu bul.
bu sorguları tekrar insan diline cevir ve bunu mu denemek istediniz diye sor
bunu mu denemek istediniz diye sor
sorulanın cevabını ver
