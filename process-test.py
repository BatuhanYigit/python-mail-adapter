# # Pandas Dask Data frame == 18
# def process_csv(file_path):
#     df = dd.read_csv(file_path)
#     df["Time series"] = dd.to_datetime(df["Time series"])

#     flights_data = df.rename(
#         columns={
#             "DepIATACtry": "OriginCountryCode",
#             "DepCity": "OriginCityCode",
#             "DepAirport": "OriginAirportCode",
#             "Carrier1": "AirlineCode",
#             "ArrIATACtry": "DestinationCountryCode",
#             "ArrCity": "DestinationCityCode",
#             "ArrAirport": "DestinationAirportCode",
#             "Seats (Total)": "Seat",
#             "Time series": "Date",
#         }
#     )

#     return flights_data.compute().to_dict(orient="records")

# # Pandass Apply ve lambda == 0:53


# def process_csv(file_path):
#     df = pd.read_csv(file_path)
#     df["Time series"] = pd.to_datetime(df["Time series"])

#     return df.apply(
#         lambda row: {
#             "OriginCountryCode": row["DepIATACtry"],
#             "OriginCityCode": row["DepCity"],
#             "OriginAirportCode": row["DepAirport"],
#             "AirlineCode": row["Carrier1"],
#             "DestinationCountryCode": row["ArrIATACtry"],
#             "DestinationCityCode": row["ArrCity"],
#             "DestinationAirportCode": row["ArrAirport"],
#             "Seat": row["Seats (Total)"],
#             "Date": row["Time series"],
#         },
#         axis=1,
#     ).tolist()
