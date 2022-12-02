# This file contains code for suporting addressing questions in the data
from .access import *

import numpy as np
import statsmodels.api as sm
import warning


"""Address a particular question that arises from the data"""

def predict_price(conn, latitude, longitude, date, property_type, size):
    """
    Price prediction for UK housing.
    
    Arguments:
      conn : Connection Object - connection to MariaDB database
      latitude : double - latitude of prediction point
      longitude: double - longitude of prediction point
      date : string - date of prediciton
      property_type : string - type of prediction property
      size : double - size of bouding box
    Output:
      
    """
 
  # Fetch Data from Database
  date_list = date.split('-')
  date_list = [int(i) for i in date_list]
  date_list[0]-=2
  from_date = f'{str(date_list[0]).zfill(4)}-{str(date_list[1]).zfill(2)}-{str(date_list[2]).zfill(2)}'
  date_list[0]+=3
  to_date = f'{str(date_list[0]).zfill(4)}-{str(date_list[1]).zfill(2)}-{str(date_list[2]).zfill(2)}'
  data_gdf = download_data_to_gdf(conn, longitude - 0.05, longitude + 0.05, latitude - 0.05, latitude + 0.05, from_date, to_date)
  # Download POIS
  pois = download_pois(latitude, longitude, 0.1)
  # Add amenity_proximity and closest_leisure Features to Data
  pois_amenity = pois[pois.amenity.notnull()]
  data_gdf['amenity_proximity'] = [pois_amenity[pois_amenity.geometry.distance(data_gdf.iloc[i].geometry)<=0.01].count().amenity for i in range(data_gdf.shape[0])]
  data_gdf['amenity_proximity'] += 1
  # Train Model with Features
  y = data_gdf[data_gdf.property_type == property_type]['price']
  if y.shape[0] != 0 :
    design = np.concatenate((np.ones(data_gdf[data_gdf.property_type == property_type].shape[0]).reshape(-1,1), 1/(data_gdf[data_gdf.property_type == property_type]['amenity_proximity']).to_numpy().reshape(-1,1)),axis=1)
    m_linear_basis = sm.OLS(y,design)
  else:
    y = data_gdf['price']
    design = np.concatenate((np.ones(data_gdf.shape[0]).reshape(-1,1), 1/(data_gdf['amenity_proximity']).to_numpy().reshape(-1,1)),axis=1)
    m_linear_basis = sm.OLS(y,design)
  results_basis = m_linear_basis.fit()
  # Get Required Features for Prediction
  df_pred = pd.DataFrame(
    {'Latitude': [latitude],
     'Longitude': [longitude]})
  gdf_pred = gpd.GeoDataFrame(df_pred, geometry=gpd.points_from_xy(df_pred.Longitude, df_pred.Latitude))
  amenity_proximity_pred = pois_amenity[pois_amenity.geometry.distance(gdf_pred.geometry[0])<=0.01].count().amenity
  design_pred = [[1, 1/(amenity_proximity_pred+1)]]
  y_pred_linear_basis = results_basis.get_prediction(design_pred).summary_frame(alpha=0.05)
  print(results_basis.summary())
  print(y_pred_linear_basis)
  pass

