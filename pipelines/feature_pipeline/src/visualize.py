import networkx as nx
import matplotlib.pyplot as plt

# Define table groups
tables = {
    "products": ["products", "product_brands", "categories", "subcategories"],
    "sales": ["orders", "order_items", "invoices", "payments"],
    "users": ["users", "customers", "admins"],
    "logistics": ["warehouses", "stock", "shipments"],
    "reference": ["status", "payment_methods"]
}

# Define foreign key relationships
relationships = [
    ("products", "order_items"),
    ("product_brands", "products"),
    ("categories", "products"),
    ("subcategories", "products"),

    ("orders", "order_items"),
    ("orders", "invoices"),
    ("order_items", "invoices"),
    ("invoices", "payments"),

    ("users", "orders"),
    ("customers", "orders"),

    ("warehouses", "stock"),
    ("products", "stock"),

    ("orders", "status"),
    ("payments", "payment_methods")
]

# Create directed graph
G = nx.DiGraph()

# Add nodes and group color coding
colors = {}
group_colors = {
    "products": "skyblue",
    "sales": "lightgreen",
    "users": "salmon",
    "logistics": "orange",
    "reference": "gray"
}
for group, tbls in tables.items():
    for table in tbls:
        G.add_node(table)
        colors[table] = group_colors[group]

# Add edges (relationships)
G.add_edges_from(relationships)

# Draw graph
plt.figure(figsize=(14, 10))
pos = nx.spring_layout(G, seed=42)
nx.draw_networkx_nodes(G, pos, node_color=[colors[n] for n in G.nodes()], node_size=1500)
nx.draw_networkx_labels(G, pos, font_size=10, font_family="sans-serif")
nx.draw_networkx_edges(G, pos, arrowstyle="->", arrowsize=20, edge_color='black', width=2)

plt.title("ERP Dataset Table Relationship Graph", fontsize=14)
plt.axis("off")
plt.tight_layout()
plt.show()
