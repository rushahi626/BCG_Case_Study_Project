from pyspark.sql.functions import col,desc,row_number
from pyspark.sql.window import Window
import pandas as pd
from src.utils import load_csv_data_df,write_output

class USVehicleAccidentAnalysis:
    '''
       Initializing constructor
    '''
    def __init__(self, spark, config):
        input_file_path = config.get('INPUT_FILENAME')
        self.df_Units = load_csv_data_df(spark, input_file_path.get('Units'))
        self.df_charge = load_csv_data_df(spark, input_file_path.get('Charges'))
        self.df_damage = load_csv_data_df(spark, input_file_path.get('Damages'))
        self.df_endorse = load_csv_data_df(spark, input_file_path.get('Endorse'))
        self.df_Restrict = load_csv_data_df(spark, input_file_path.get('Restrict'))
        self.df_primaryPerson = load_csv_data_df(spark, input_file_path.get('Primary_Person'))
        
        '''
         Defining methods which will actually fetch the result
        '''
    def males_killed_count(self,output_path):
        '''
          Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2
          function gonna return count of crashes in which males killed > 2
        '''
        df_malesKilledCount = self.df_primaryPerson.filter((col('PRSN_GNDR_ID') == 'MALE') & (col('DEATH_CNT') > 2))
        write_output(df_malesKilledCount,output_path)
        return df_malesKilledCount.count()
    
    def no_two_wheelers_crashed(self,output_path):
        '''
           How many two wheelers are booked for crashes?
        '''
        two_wheelers_df = self.df_Units.filter(col('VEH_BODY_STYL_ID').contains('MOTORCYCLE'))
        write_output(two_wheelers_df,output_path)
        return two_wheelers_df.count()
    
    def top5_carsMakers_crash(self,output_path):
        '''
          Analysis 3:Determine the Top5 Vehicle Makes of the cars present in the crashes in 
          which driver died and Airbags did not deploy
          
          keeping vehicle body style with CAR only(mentioned in question only)
          
          method gonna return List[str] of top 5 Car maker for killed crashes without an airbag deployed with list[0] highest value
        '''
        
        df_top5CarMakers = (
        self.df_Units
        .join(self.df_primaryPerson, how='inner', on=['CRASH_ID'])
        .filter(
            (col('VEH_BODY_STYL_ID').contains('CAR')) &
            (col('VEH_MAKE_ID') != 'NA') &
            (col('PRSN_INJRY_SEV_ID') == 'KILLED') &
            (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED')
        )
        .groupBy('VEH_MAKE_ID')
        .count()
        .orderBy(desc('count'))
        .limit(5)
        )
        
        write_output(df_top5CarMakers,output_path)
        return [row[0] for row in df_top5CarMakers.collect()]
    
    def lic_veh_HNR_count(self,output_path):
        '''
          Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run?
          method return count of vehicles with valid DL driver involved in hit and run
          
        '''
        df_HnrCnt = (
        self.df_Units.select("CRASH_ID", "VEH_HNR_FL")
        .join(self.df_primaryPerson.select("CRASH_ID", "DRVR_LIC_TYPE_ID"), how='inner', on=['CRASH_ID'])
        .filter(
            (col('VEH_HNR_FL') == 'Y') &
            (col('DRVR_LIC_TYPE_ID').isin('DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'))
        )
        )
        
        write_output(df_HnrCnt,output_path)
        return df_HnrCnt.count()
    
    def state_with_no_female_acdnt(self,output_path):
        '''
           Analysis 5: Which state has highest number of accidents in which females are not involved?
           considering only state with valid names...ignoring state with names NA,Other,Unknown
           method returns string (str) as only first state from the dataframe
           
        '''
        df_StateMaxFemale = (
        self.df_primaryPerson
        .select('DRVR_LIC_STATE_ID')
        .filter(
            (col('PRSN_GNDR_ID') != 'FEMALE') &
            (~col('DRVR_LIC_STATE_ID').isin('NA', 'Other', 'Unknown'))
        )
        .groupBy('DRVR_LIC_STATE_ID')
        .count()
        .orderBy(desc('count'))
        )
        write_output(df_StateMaxFemale,output_path)
        return df_StateMaxFemale.first().DRVR_LIC_STATE_ID
    
    def top3to5_vehMake_injury_death(self,output_path):
        '''
          Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
          TOTAL_INJRY_CNT = POSS_INJRY_CNT + INCAP_INJRY_CNT + NONINCAP_INJRY_CNT+...
          Ignoring vehicle maker Id as NA
          gonna find out top 3 to 5th rank using window function row_number
          method returns list[str] of Vehicle Makers
        '''
        
        window_spec = Window.orderBy(desc('TOT_INJRY_DEATH_VEH_WISE'))
        
        df_total_InjryDeath = (
        self.df_Units
        .filter(col('VEH_MAKE_ID') != 'NA')
        .withColumn('TOTAL_INJRY_DEATH', col('TOT_INJRY_CNT') + col('DEATH_CNT'))
        .groupBy('VEH_MAKE_ID')
        .sum('TOTAL_INJRY_DEATH')
        .withColumnRenamed('sum(TOTAL_INJRY_DEATH)', 'TOT_INJRY_DEATH_VEH_WISE')
        .withColumn('rnk', row_number().over(window_spec))
        .filter((col('rnk') >= 3) & (col('rnk') <= 5))
        .drop('rnk'))
        
        write_output(df_total_InjryDeath,output_path)
        return [vehicle[0] for vehicle in df_total_InjryDeath.select("VEH_MAKE_ID").collect()]
    
    def top_ethnic_grp_crash_body_style_wise(self,output_path):
        
        '''
          Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
          filtering out Vehicle body style with value NA, UNKNOWN, OTHER  (EXPLAIN IN NARRATIVE) ,NOT REPORTED
          filter out ethnic group with names NA,Unknown
          
          method return the result into dictionary format
        '''
        df_ethnic_grp_body_wise = (
        self.df_Units
        .join(self.df_primaryPerson, how='inner', on=['CRASH_ID'])
        .filter(
            (~col('VEH_BODY_STYL_ID').isin(['NA', 'UNKNOWN', 'OTHER  (EXPLAIN IN NARRATIVE)', 'NOT REPORTED'])) &
            (~col('PRSN_ETHNICITY_ID').isin(['NA', 'Unknown']))
        )
        .groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')
        .count()
        )
        
        window_spec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(desc('count'))
        df_ethnic_grp_body_wise = (
        df_ethnic_grp_body_wise
        .withColumn('rnk', row_number().over(window_spec))
        .filter(col('rnk') == 1)
        .drop('count','rnk')
        )
        write_output(df_ethnic_grp_body_wise,output_path)

        result = df_ethnic_grp_body_wise.select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').collect()
        result_dict = {row['VEH_BODY_STYL_ID']: row['PRSN_ETHNICITY_ID'] for row in result}
        return result_dict
    
    
    def top5_zip_cd_with_max_OH_crash(self,output_path):
        
        '''
          Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols 
                      as the contributing factor to a crash (Use Driver Zip Code)
          
          Considering only valid Zip codes, removing Null ones 
          method return List[str] of top 5 driver's zip code ...
        '''
        df_Top5zipCD_crash = (
            self.df_primaryPerson
            .join(self.df_Units, on='CRASH_ID', how='inner')
            .filter(
                (col('CONTRIB_FACTR_1_ID').contains('ALCOHOL')) |
                (col('CONTRIB_FACTR_2_ID').contains('ALCOHOL'))
            )
            .filter(col('DRVR_ZIP').isNotNull())
            .groupBy('DRVR_ZIP')
            .count()
            .orderBy(desc('count'))
            .limit(5).drop('count'))
        
        write_output(df_Top5zipCD_crash,output_path)
        return [row[0] for row in df_Top5zipCD_crash.collect()]
    
    def dist_crash_ids_with_insurance(self,output_path):
        
        '''
          Analysis 9: Count of Distinct Crash IDs 
                where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
        '''
        df_crash_id_damage_ge4 = (
            self.df_damage
            .join(self.df_Units, on='CRASH_ID', how='inner')
            .filter(col('VEH_DMAG_SCL_1_ID').isin('DAMAGED 5', 'DAMAGED 6', 'DAMAGED 7 HIGHEST'))
            .filter((col('DAMAGED_PROPERTY') == 'NONE') & (col('FIN_RESP_TYPE_ID') == 'PROOF OF LIABILITY INSURANCE'))
            .select('CRASH_ID')
            .distinct()
        )
        
        write_output(df_crash_id_damage_ge4,output_path)
        return df_crash_id_damage_ge4.count()
    
    def top5_vehmaker_licDriver_tencolor_25states(self,output_path):
        '''
          Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, 
          has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with 
          highest number of offences
          
          approach - will get top 25 states in list[str] and top 10 used vehicle color in list[str]
        '''
        top_25_States = [
            row[0]
            for row in self.df_Units.filter(
                (col("VEH_LIC_STATE_ID").rlike("^[^0-9]+$")) & (col('VEH_LIC_STATE_ID')!='NA')
            )
            .groupby("VEH_LIC_STATE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(25)
            .collect()
        ]
        
        top10_used_veh_color = [
            row[0]
            for row in self.df_Units.filter(col('VEH_COLOR_ID') != "NA")
            .groupby("VEH_COLOR_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        ]
        
        df_top5Veh_color_state = (
            self.df_charge.join(self.df_primaryPerson, on=["CRASH_ID"], how="inner")
            .join(self.df_Units, on=["CRASH_ID"], how="inner")
            .filter(col('CHARGE').contains("SPEED"))
            .filter(
                col('DRVR_LIC_TYPE_ID').isin(
                    ["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."]
                )
            )
            .filter(col('VEH_COLOR_ID').isin(top10_used_veh_color))
            .filter(col('VEH_LIC_STATE_ID').isin(top_25_States))
            .groupby("VEH_MAKE_ID")
            .count()
            .orderBy(col("count").desc())
            .limit(5)
        )
        write_output(df_top5Veh_color_state,output_path)
        return [row[0] for row in df_top5Veh_color_state.collect()]
    