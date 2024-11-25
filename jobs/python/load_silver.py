# Zapisanie tabeli autorów do lokalnego folderu
authors_path = os.path.join(output_dir, 'authors.csv')
authors_df.toPandas().to_csv(authors_path, index=False)
print(f"Zapisano tabelę autorów do: {authors_path}")


# Zapisanie tabeli publikacji do lokalnego folderu
publications_path = os.path.join(output_dir, 'publications.csv')
publications_df.toPandas().to_csv(publications_path, index=False)
print(f"Zapisano tabelę publikacji do: {publications_path}")

# Zapisanie tabeli jednostek chorobowych do lokalnego folderu
diseases_path = os.path.join(output_dir, 'diseases.csv')
diseases_df.toPandas().to_csv(diseases_path, index=False)
print(f"Zapisano tabelę jednostek chorobowych do: {diseases_path}")
