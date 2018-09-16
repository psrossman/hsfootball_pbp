library(RMySQL)
con <- dbConnect(RMySQL::MySQL(), host = "localhost", user = "root", password = "root", dbname="prod_football")

rs <- dbSendQuery(con, "select id, Date, a.GameID, a.play_id, Drive, cast(qtr as int) as qtr, TimeSecs, posteam, DefensiveTeam, if(down = 0,null,down) as down, case when play_type in ('End of Quarter', 'End of Half', 'End of Game') then 0 when play_type in ('Extra Point') then 3 when play_type in ('Kickoff') then 0 else ydstogo end as ydstogo, case when yrdline100 = 0 then null else yrdline100 end as yrdline100, play_type, net_yards, if(ydstogo = yrdline100,1,0) as GoalToGo, FirstDown, sp, if(Touchdown = 1,1,0) as Touchdown, if(ExPointResult = '',null,ExPointResult) as ExPointResult, if(TwoPointConv = '', null,TwoPointConv) as TwoPointConv, Fumble, RecFumbTeam, PosTeamScore, DefTeamScore, PosTeamScore-DefTeamScore as ScoreDiff, abs(PosTeamScore-DefTeamScore) as AbsScoreDiff, If(Safety = 1, 1,0) as Safety, FieldGoalResult, case when trim(ReturnResult) like '%touchdown%' then 'Touchdown' else null end as ReturnResult, if(Result = 'Interception',1,0) as InterceptionThrown, HomeTeam, AwayTeam from prod_football.lv_pbp a
left outer join (select a.GameID, play_id, a.half, case when a.half = 1 then (rn/cnt*1440)+1440 when a.half = 2 then (rn/cnt*1440) end as TimeSecs from
                  (
                  select GameID, play_id, case when qtr in (1,2) then 1 when qtr in (3,4) then 2 else 3 end as half
                  , rank() over (partition by GameID, half order by play_id desc) as rn
                  from lv_pbp where (down in (1,2,3,4) or play_type in ('Kickoff')) and play_type <> 'No Play'
                  group by 1,2,3) a
                  inner join (select GameID, case when qtr in (1,2) then 1 when qtr in (3,4) then 2 else 3 end as half, count(distinct play_id) as cnt
                  from lv_pbp where (down in (1,2,3,4) or play_type in ('Kickoff')) and play_type <> 'No Play'
                  group by 1,2) b
                  on a.GameID = b.GameID
                  and a.half = b.half) b
                  on a.GameID = b.GameID
                  and a.play_id = b.play_id
                  where a.play_type <> 'No Play'")

data1 <- dbFetch(rs, n = -1) 

lv_pbp <- data1

lv_pbp$log_ydstogo <- log(lv_pbp$ydstogo);

lv_pbp$down <- as.factor(lv_pbp$down);

repeat_last = function(x, forward = TRUE, maxgap = Inf, na.rm = FALSE) {
  if (!forward) x = rev(x)           # reverse x twice if carrying backward
  ind = which(!is.na(x))             # get positions of nonmissing values
  if (is.na(x[1]) && !na.rm)         # if it begins with NA
    ind = c(1,ind)                 # add first pos
  rep_times = diff(                  # diffing the indices + length yields how often
    c(ind, length(x) + 1) )          # they need to be repeated
  if (maxgap < Inf) {
    exceed = rep_times - 1 > maxgap  # exceeding maxgap
    if (any(exceed)) {               # any exceed?
      ind = sort(c(ind[exceed] + 1, ind))      # add NA in gaps
      rep_times = diff(c(ind, length(x) + 1) ) # diff again
    }
  }
  x = rep(x[ind], times = rep_times) # repeat the values at these indices
  if (!forward) x = rev(x)           # second reversion
  x
}

lv_pbp$TimeSecs <- repeat_last(lv_pbp$TimeSecs)

lv_pbp$TimeSecs_Remaining <- ifelse(lv_pbp$qtr %in% c(1,2),
                                    lv_pbp$TimeSecs - 1440,
                                    lv_pbp$TimeSecs)

lv_pbp$yrdline100 <- repeat_last(lv_pbp$yrdline100)


library(tidyverse)
library(nflWAR)
library(sqldf)
options(sqldf.driver = "SQLite")

pbp_data <- lv_pbp
nrow(pbp_data)


find_game_next_score_half <- function(pbp_dataset) {
  
  # Which rows are the scoring plays:
  score_plays <- which(pbp_dataset$sp == 1 & pbp_dataset$play_type != "No Play")
  
  # Define a helper function that takes in the current play index, 
  # a vector of the scoring play indices, play-by-play data,
  # and returns the score type and drive number for the next score:
  find_next_score <- function(play_i, score_plays_i,pbp_df) {
    
    # Find the next score index for the current play
    # based on being the first next score index:
    next_score_i <- score_plays_i[which(score_plays_i >= play_i)[1]]
    
    # If next_score_i is NA (no more scores after current play)
    # or if the next score is in another half,
    # then return No_Score and the current drive number
    if (is.na(next_score_i) | 
        (pbp_df$qtr[play_i] %in% c(1, 2) & pbp_df$qtr[next_score_i] %in% c(3, 4, 5)) | 
        (pbp_df$qtr[play_i] %in% c(3, 4) & pbp_df$qtr[next_score_i] == 5)) {
      
      score_type <- "No_Score"
      
      # Make it the current play index
      score_drive <- pbp_df$Drive[play_i]
      
      # Else return the observed next score type and drive number:
    } else {
      
      # Store the score_drive number
      score_drive <- pbp_df$Drive[next_score_i]
      
      # Then check the play types to decide what to return
      # based on several types of cases for the next score:
      
      # 1: Return TD
      if (identical(pbp_df$ReturnResult[next_score_i], "Touchdown")) {
        
        # For return touchdowns the current posteam would not have
        # possession at the time of return, so it's flipped:
        if (identical(pbp_df$posteam[play_i], pbp_df$posteam[next_score_i])) {
          
          score_type <- "Opp_Touchdown"
          
        } else {
          
          score_type <- "Touchdown"
          
        }
      } else if (identical(pbp_df$FieldGoalResult[next_score_i], "Good")) {
        
        # 2: Field Goal
        # Current posteam made FG
        if (identical(pbp_df$posteam[play_i], pbp_df$posteam[next_score_i])) {
          
          score_type <- "Field_Goal"
          
          # Opponent made FG
        } else {
          
          score_type <- "Opp_Field_Goal"
          
        }
        
        # 3: Touchdown (returns already counted for)
      } else if (pbp_df$Touchdown[next_score_i] == 1) {
        
        # Current posteam TD
        if (identical(pbp_df$posteam[play_i], pbp_df$posteam[next_score_i])) {
          
          score_type <- "Touchdown"
          
          # Opponent TD
        } else {
          
          score_type <- "Opp_Touchdown"
          
        }
        # 4: Safety (similar to returns)
      } else if (pbp_df$Safety[next_score_i] == 1) {
        
        if (identical(pbp_df$posteam[play_i],pbp_df$posteam[next_score_i])) {
          
          score_type <- "Opp_Safety"
          
        } else {
          
          score_type <- "Safety" 
          
        }
        # 5: Extra Points
      } else if (identical(pbp_df$ExPointResult[next_score_i], "Made")) {
        
        # Current posteam Extra Point
        if (identical(pbp_df$posteam[play_i], pbp_df$posteam[next_score_i])) {
          
          score_type <- "Extra_Point"
          
          # Opponent Extra Point
        } else {
          
          score_type <- "Opp_Extra_Point"
          
        }
        # 6: Two Point Conversions
      } else if (identical(pbp_df$TwoPointConv[next_score_i], "Success")) {
        
        # Current posteam Two Point Conversion
        if (identical(pbp_df$posteam[play_i], pbp_df$posteam[next_score_i])) {
          
          score_type <- "Two_Point_Conversion"
          
          # Opponent Two Point Conversion
        } else {
          
          score_type <- "Opp_Two_Point_Conversion"
          
        }
        
        # 7: Defensive Two Point (like returns)
      } else if (identical(pbp_df$DefTwoPoint[next_score_i], "Success")) {
        
        if (identical(pbp_df$posteam[play_i], pbp_df$posteam[next_score_i])) {
          
          score_type <- "Opp_Defensive_Two_Point"
          
        } else {
          
          score_type <- "Defensive_Two_Point"
          
        }
        
        # 8: Errors of some sort so return NA (but shouldn't take place)
      } else {
        
        score_type <- NA
        
      }
    }
    
    return(data.frame(Next_Score_Half = score_type,
                      Drive_Score_Half = score_drive))
  }
  
  # Using lapply and then bind_rows is much faster than
  # using map_dfr() here:
  lapply(c(1:nrow(pbp_dataset)), find_next_score, 
         score_plays_i = score_plays, pbp_df = pbp_dataset) %>%
    bind_rows() %>%
    return
}

pbp_next_score_half <- map_dfr(unique(pbp_data$GameID), 
                               function(x) {
                                 pbp_data %>%
                                   filter(GameID == x) %>%
                                   find_game_next_score_half()
                               })




# Join to the pbp_data:
pbp_data_next_score <- bind_cols(pbp_data, pbp_next_score_half)

# Create the EP model dataset that only includes plays with basic seven 
# types of next scoring events along with the following play types:
# Field Goal, No Play, Pass, Punt, Run, Sack, Spike

pbp_ep_model_data <- pbp_data_next_score %>% 
  filter(Next_Score_Half %in% c("Opp_Field_Goal", "Opp_Safety", "Opp_Touchdown",
                                "Field_Goal", "No_Score", "Safety", "Touchdown") & 
           play_type %in% c("Field Goal", "No Play", "Pass", "Punt", "Run", "Sack",
                           "Spike") & is.na(TwoPointConv) & is.na(ExPointResult) &
           !is.na(down) & !is.na(TimeSecs))
nrow(pbp_ep_model_data)
# 304805

pbp_ep_model_data <- sqldf("select * from pbp_ep_model_data where TwoPointConv is null")

# Now adjust and create the model variables:
pbp_ep_model_data <- pbp_ep_model_data %>%
  
  # Reference level should be No_Score:
  mutate(Next_Score_Half = fct_relevel(factor(Next_Score_Half), "No_Score"),
         
         # Create a variable that is time remaining until end of half:
         # (only working with up to 2016 data so can ignore 2017 time change)
               TimeSecs_Remaining = as.numeric(ifelse(qtr %in% c(1,2), TimeSecs - 1440,
                                                             TimeSecs)),
         
         # log transform of yards to go and indicator for two minute warning:
         log_ydstogo = log(ydstogo),
             Under_TwoMinute_Warning = ifelse(TimeSecs_Remaining < 120, 1, 0),
         
         # Changing down into a factor variable: 
         down = factor(down),
         
         # Calculate the drive difference between the next score drive and the 
         # current play drive:
         Drive_Score_Dist = Drive_Score_Half - Drive,
         
         # Create a weight column based on difference in drives between play and next score:
         Drive_Score_Dist_W = (max(Drive_Score_Dist) - Drive_Score_Dist) / 
           (max(Drive_Score_Dist) - min(Drive_Score_Dist)),
         # Create a weight column based on score differential:
         ScoreDiff_W = (max(abs(ScoreDiff)) - abs(ScoreDiff)) / 
           (max(abs(ScoreDiff)) - min(abs(ScoreDiff))),
         # Add these weights together and scale again:
         Total_W = Drive_Score_Dist_W + ScoreDiff_W,
         Total_W_Scaled = (Total_W - min(Total_W)) / 
           (max(Total_W) - min(Total_W)))

# Save dataset in data folder as pbp_ep_model_data.csv
# (NOTE: this dataset is not pushed due to its size exceeding
# the github limit but will be referenced in other files)
# write_csv(pbp_ep_model_data, "data/pbp_ep_model_data.csv")

# Fit the expected points model:
# install.packages("nnet")
ep_model <- nnet::multinom(Next_Score_Half ~ TimeSecs_Remaining + yrdline100 + 
                             down + log_ydstogo + GoalToGo + log_ydstogo*down + 
                             yrdline100*down + GoalToGo*log_ydstogo, data = pbp_ep_model_data, 
                           weights = Total_W_Scaled, maxit = 300, na.action = na.pass)

fg_model_data <-  pbp_data_next_score %>% 
  filter(play_type %in% c("Field Goal","Extra Point") & 
           (!is.na(ExPointResult) | !is.na(FieldGoalResult)))

nrow(fg_model_data)
# 16906

# Save dataset in data folder as fg_model_data.csv
# (NOTE: this dataset is not pushed due to its size exceeding
# the github limit but will be referenced in other files)
# write_csv(fg_model_data, "data/fg_model_data.csv")

# Fit the field goal model:
# install.packages("mgcv")
fg_model <- mgcv::bam(sp ~ s(yrdline100), 
                      data = fg_model_data, family = "binomial")

base_ep_preds <- as.data.frame(predict(ep_model, newdata = pbp_ep_model_data, type = "probs"))

colnames(base_ep_preds) <- c("No_Score","Field_Goal","Opp_Field_Goal", "Opp_Safety","Opp_Touchdown","Safety","Touchdown")

base_ep_preds <- dplyr::rename(base_ep_preds,Field_Goal_Prob=Field_Goal,Touchdown_Prob=Touchdown,
                               Opp_Field_Goal_Prob=Opp_Field_Goal,Opp_Touchdown_Prob=Opp_Touchdown,
                               Safety_Prob=Safety,Opp_Safety_Prob=Opp_Safety,No_Score_Prob=No_Score)  


pbp_ep_model_data <- cbind(pbp_ep_model_data, base_ep_preds)

lv_pbp2 <- sqldf("select a.id, a.Date, a.GameID, a.play_id, a.Drive, a.qtr, a.TimeSecs, a.TimeSecs_Remaining, a.posteam, a.DefensiveTeam, a.down, a.ydstogo, a.yrdline100, a.play_type, a.net_yards, a.GoalToGo, a.FirstDown, a.sp, a.Touchdown, a.ExPointResult, a.TwoPointConv, a.Fumble, a.RecFumbTeam, a.PosTeamScore, a.DefTeamScore, a.ScoreDiff, a.AbsScoreDiff, a.Safety, a.FieldGoalResult, a.ReturnResult, a.InterceptionThrown, a.HomeTeam, a.AwayTeam, Field_Goal_Prob, Touchdown_Prob, Opp_Field_Goal_Prob, Opp_Touchdown_Prob, Safety_Prob, Opp_Safety_prob, No_Score_Prob from lv_pbp a left outer join pbp_ep_model_data b on a.id = b.id")


  # Calculate the EP for receiving a touchback (from the point of view for recieving team)
  # and update the columns for Kickoff plays:
  kickoff_data <- lv_pbp2 
  
  # Change the yard line to be 80 for 2009-2015 and 75 otherwise
  # (accounting for the fact that Jan 2016 is in the 2015 season:
  kickoff_data$yrdline100 <- 60;
  
  # Not GoalToGo:
  kickoff_data$GoalToGo <- rep(0,nrow(lv_pbp2))
  # Now first down:
  kickoff_data$down <- rep("1",nrow(lv_pbp2))
  # 10 ydstogo:
  kickoff_data$ydstogo <- rep(10,nrow(lv_pbp2))
  # Create log_ydstogo:
  kickoff_data <- dplyr::mutate(kickoff_data, log_ydstogo = log(ydstogo))
  
  # Get the new predicted probabilites:
  if (nrow(kickoff_data) > 1) {
    kickoff_preds <- as.data.frame(predict(ep_model, newdata = kickoff_data, type = "probs"))
  } else{
    kickoff_preds <- as.data.frame(matrix(predict(ep_model, newdata = kickoff_data, type = "probs"),
                                          ncol = 7))
  }
  colnames(kickoff_preds) <- c("No_Score","Opp_Field_Goal","Opp_Safety","Opp_Touchdown",
                               "Field_Goal","Safety","Touchdown")
  # Find the kickoffs:
  kickoff_i <- which(lv_pbp2$play_type == "Kickoff")
  
  # Now update the probabilities:
  lv_pbp2[kickoff_i, "Field_Goal_Prob"] <- kickoff_preds[kickoff_i, "Field_Goal"]
  lv_pbp2[kickoff_i, "Touchdown_Prob"] <- kickoff_preds[kickoff_i, "Touchdown"]
  lv_pbp2[kickoff_i, "Opp_Field_Goal_Prob"] <- kickoff_preds[kickoff_i, "Opp_Field_Goal"]
  lv_pbp2[kickoff_i, "Opp_Touchdown_Prob"] <- kickoff_preds[kickoff_i, "Opp_Touchdown"]
  lv_pbp2[kickoff_i, "Safety_Prob"] <- kickoff_preds[kickoff_i, "Safety"]
  lv_pbp2[kickoff_i, "Opp_Safety_Prob"] <- kickoff_preds[kickoff_i, "Opp_Safety"]
  lv_pbp2[kickoff_i, "No_Score_Prob"] <- kickoff_preds[kickoff_i, "No_Score"]
  
  # ----------------------------------------------------------------------------------
  # Insert probabilities of 0 for everything but No_Score for QB Kneels:
  # Find the QB Kneels:
  qb_kneels_i <- which(lv_pbp2$play_type == "QB Kneel")
  
  # Now update the probabilities:
  lv_pbp2[qb_kneels_i, "Field_Goal_Prob"] <- 0
  lv_pbp2[qb_kneels_i, "Touchdown_Prob"] <- 0
  lv_pbp2[qb_kneels_i, "Opp_Field_Goal_Prob"] <- 0
  lv_pbp2[qb_kneels_i, "Opp_Touchdown_Prob"] <- 0
  lv_pbp2[qb_kneels_i, "Safety_Prob"] <- 0
  lv_pbp2[qb_kneels_i, "Opp_Safety_Prob"] <- 0
  lv_pbp2[qb_kneels_i, "No_Score_Prob"] <- 1
  
  
  # ----------------------------------------------------------------------------------
  # Create two new columns, ExPoint_Prob and TwoPoint_Prob, for the PAT events:
  lv_pbp2$ExPoint_Prob <- 0
  lv_pbp2$TwoPoint_Prob <- 0
  
  # Find the indices for these types of plays:
  extrapoint_i <- which(!is.na(lv_pbp2$ExPointResult))
  twopoint_i <- which(!is.na(lv_pbp2$TwoPointConv))
  
  expt_df <- sqldf("select avg(case when ExPointResult = 'Made' then 1 else 0 end) as avg_expt from lv_pbp2 where ExPointResult is not null")
  
  twopt_df <- sqldf("select avg(case when TwoPointConv = 'Success' then 1 else 0 end) as avg_twopt from lv_pbp2 where TwoPointConv is not null")
  
  # Assign the make_fg_probs of the extra-point PATs:
  lv_pbp2$ExPoint_Prob[extrapoint_i] <- expt_df$avg_expt
  
  # Assign the TwoPoint_Prob with the historical success rate:
  lv_pbp2$TwoPoint_Prob[twopoint_i] <- twopt_df$avg_twopt
  
  # ----------------------------------------------------------------------------------
  # Insert NAs for all other types of plays:
  missing_i <- which(lv_pbp2$play_type %in% c("End of Quarter", "End of Game", "End of Half"))
  
  # Now update the probabilities for missing and PATs:
  lv_pbp2$Field_Goal_Prob[c(missing_i,extrapoint_i,twopoint_i)] <- 0
  lv_pbp2$Touchdown_Prob[c(missing_i,extrapoint_i,twopoint_i)] <- 0
  lv_pbp2$Opp_Field_Goal_Prob[c(missing_i,extrapoint_i,twopoint_i)] <- 0
  lv_pbp2$Opp_Touchdown_Prob[c(missing_i,extrapoint_i,twopoint_i)] <- 0
  lv_pbp2$Safety_Prob[c(missing_i,extrapoint_i,twopoint_i)] <- 0
  lv_pbp2$Opp_Safety_Prob[c(missing_i,extrapoint_i,twopoint_i)] <- 0
  lv_pbp2$No_Score_Prob[c(missing_i,extrapoint_i,twopoint_i)] <- 0


  lv_pbp2 <- dplyr::mutate(lv_pbp2,
                               ExpPts = (0*No_Score_Prob) + (-3 * Opp_Field_Goal_Prob) + 
                                 (-2 * Opp_Safety_Prob) +
                                 (-7 * Opp_Touchdown_Prob) + (3 * Field_Goal_Prob) +
                                 (2 * Safety_Prob) + (7 * Touchdown_Prob) +
                                 (1 * ExPoint_Prob) + (2 * TwoPoint_Prob))

pbp_data_ep <- lv_pbp2


pbp_data_epa <- dplyr::group_by(pbp_data_ep,GameID)
pbp_data_epa <- dplyr::mutate(pbp_data_epa,
                              # Offense touchdown (including kickoff returns):
                              EPA_off_td = 7 - ExpPts,
                              # Offense fieldgoal:
                              EPA_off_fg = 3 - ExpPts,
                              # Offense extra-point conversion:
                              EPA_off_ep = 1 - ExpPts,
                              # Offense two-point conversion:
                              EPA_off_tp = 2 - ExpPts,
                              # Missing PAT:
                              EPA_PAT_fail = 0 - ExpPts,
                              # Opponent Safety:
                              EPA_safety = -2 - ExpPts,
                              # End of half/game or timeout or QB Kneel:
                              EPA_endtime = 0,
                              # Defense scoring touchdown (including punt returns):
                              EPA_change_score = -7 - ExpPts,
                              # Change of possession without defense scoring
                              # and no timeout, two minute warning, or quarter end follows:
                              EPA_change_no_score = -dplyr::lead(ExpPts) - ExpPts,
                              # Change of possession without defense scoring
                              # but either Timeout, Two Minute Warning,
                              # Quarter End is the following row:
                              EPA_change_no_score_nxt = -dplyr::lead(ExpPts,2) - ExpPts,
                              # Team keeps possession but either Timeout, Two Minute Warning,
                              # Quarter End is the following row:
                              EPA_base_nxt = dplyr::lead(ExpPts,2) - ExpPts,
                              # Team keeps possession (most general case):
                              EPA_base = dplyr::lead(ExpPts) - ExpPts,
                              # Now the same for PTDA:
                              # Team keeps possession (most general case):
                              PTDA_base = dplyr::lead(Touchdown_Prob) - Touchdown_Prob,
                              # Team keeps possession but either Timeout, Two Minute Warning,
                              # Quarter End is the following row:
                              PTDA_base_nxt = dplyr::lead(Touchdown_Prob,2) - Touchdown_Prob,
                              # Change of possession without defense scoring
                              # and no timeout, two minute warning, or quarter end follows:
                              PTDA_change_no_score = dplyr::lead(Opp_Touchdown_Prob) - Touchdown_Prob,
                              # Change of possession without defense scoring
                              # but either Timeout, Two Minute Warning,
                              # Quarter End is the following row:
                              PTDA_change_no_score_nxt = dplyr::lead(Opp_Touchdown_Prob,2) - Touchdown_Prob,
                              # Change of possession with defense scoring touchdown:
                              PTDA_change_score = 0 - Touchdown_Prob,
                              # Offense touchdown:
                              PTDA_off_td = 1 - Touchdown_Prob,
                              # Offense fieldgoal:
                              PTDA_off_fg = 0 - Touchdown_Prob,
                              # Offense extra-point conversion:
                              PTDA_off_ep = 0,
                              # Offense two-point conversion:
                              PTDA_off_tp = 0,
                              # Offense PAT fail:
                              PTDA_PAT_fail = 0,
                              # Opponent Safety:
                              PTDA_safety = 0 - Touchdown_Prob,
                              # End of half/game or timeout or QB Kneel:
                              PTDA_endtime = 0)


# Define the scoring plays first:
pbp_data_epa$EPA_off_td_ind <- with(pbp_data_epa, 
                                    ifelse(sp == 1 & Touchdown == 1 & 
                                             ((play_type %in% c("Pass","Run") &
                                                 (InterceptionThrown != 1 & 
                                                    Fumble != 1)) | (play_type == "Kickoff" &
                                                                       ReturnResult == "Touchdown")), 1,0))
pbp_data_epa$EPA_off_fg_ind <- with(pbp_data_epa,
                                    ifelse(play_type %in% c("Field Goal","Run") &
                                             FieldGoalResult == "Good", 1, 0))
pbp_data_epa$EPA_off_ep_ind <- with(pbp_data_epa,
                                    ifelse(ExPointResult == "Made" & play_type != "No Play", 1, 0))
pbp_data_epa$EPA_off_tp_ind <- with(pbp_data_epa,
                                    ifelse(TwoPointConv == "Success" & play_type != "No Play", 1, 0))
pbp_data_epa$EPA_PAT_fail_ind <- with(pbp_data_epa,
                                      ifelse(play_type != "No Play" & (ExPointResult %in% c("Missed", "Aborted", "Blocked") | 
                                                                         TwoPointConv == "Failure"), 1, 0))
pbp_data_epa$EPA_safety_ind <- with(pbp_data_epa,
                                    ifelse(play_type != "No Play" & Safety == 1, 1, 0))
pbp_data_epa$EPA_endtime_ind <- with(pbp_data_epa,
                                     ifelse(play_type %in% c("Half End","Quarter End",
                                                             "End of Game","Timeout","QB Kneel") | 
                                              (GameID == dplyr::lead(GameID) & sp != 1 & 
                                                 Touchdown != 1 & 
                                                 is.na(FieldGoalResult) & is.na(ExPointResult) & 
                                                 is.na(TwoPointConv) & Safety != 1 & 
                                                 ((dplyr::lead(play_type) %in% c("Half End","End of Game")) | 
                                                    (qtr == 2 & dplyr::lead(qtr)==3) | 
                                                    (qtr == 4 & dplyr::lead(qtr)==5))),1,0))
pbp_data_epa$EPA_change_score_ind <- with(pbp_data_epa,
                                          ifelse(play_type != "No Play" & sp == 1 & 
                                                   Touchdown == 1 & 
                                                   (InterceptionThrown == 1 | 
                                                      (Fumble == 1 & RecFumbTeam != posteam) |
                                                      (play_type == "Punt" & ReturnResult == "Touchdown")), 1, 0))
pbp_data_epa$EPA_change_no_score_nxt_ind <- with(pbp_data_epa,
                                                 ifelse(GameID == dplyr::lead(GameID) & 
                                                          GameID == dplyr::lead(GameID,2) &
                                                          sp != 1  & 
                                                          dplyr::lead(play_type) %in% c("Quarter End",
                                                                                        "Two Minute Warning",
                                                                                        "Timeout") &
                                                          (Drive != dplyr::lead(Drive,2)) &
                                                          (posteam != dplyr::lead(posteam,2)), 1, 0))
pbp_data_epa$EPA_base_nxt_ind <- with(pbp_data_epa,
                                      ifelse(GameID == dplyr::lead(GameID) & 
                                               GameID == dplyr::lead(GameID,2) &
                                               sp != 1  & 
                                               dplyr::lead(play_type) %in% c("Quarter End",
                                                                             "Two Minute Warning",
                                                                             "Timeout") &
                                               (Drive == dplyr::lead(Drive,2)), 1, 0))
pbp_data_epa$EPA_change_no_score_ind <- with(pbp_data_epa,
                                             ifelse(GameID == dplyr::lead(GameID) & 
                                                      Drive != dplyr::lead(Drive) &
                                                      posteam != dplyr::lead(posteam) &
                                                      dplyr::lead(play_type) %in% 
                                                      c("Pass","Run","Punt","Sack",
                                                        "Field Goal","No Play",
                                                        "QB Kneel","Spike"), 1, 0))

# Replace the missings with 0 due to how ifelse treats missings
pbp_data_epa$EPA_PAT_fail_ind[is.na(pbp_data_epa$EPA_PAT_fail_ind)] <- 0 
pbp_data_epa$EPA_base_nxt_ind[is.na(pbp_data_epa$EPA_base_nxt_ind)] <- 0
pbp_data_epa$EPA_change_no_score_nxt_ind[is.na(pbp_data_epa$EPA_change_no_score_nxt_ind)] <- 0
pbp_data_epa$EPA_change_no_score_ind[is.na(pbp_data_epa$EPA_change_no_score_ind)] <- 0 
pbp_data_epa$EPA_change_score_ind[is.na(pbp_data_epa$EPA_change_score_ind)] <- 0
pbp_data_epa$EPA_off_td_ind[is.na(pbp_data_epa$EPA_off_td_ind)] <- 0
pbp_data_epa$EPA_off_fg_ind[is.na(pbp_data_epa$EPA_off_fg_ind)] <- 0
pbp_data_epa$EPA_off_ep_ind[is.na(pbp_data_epa$EPA_off_ep_ind)] <- 0
pbp_data_epa$EPA_off_tp_ind[is.na(pbp_data_epa$EPA_off_tp_ind)] <- 0
pbp_data_epa$EPA_safety_ind[is.na(pbp_data_epa$EPA_safety_ind)] <- 0
pbp_data_epa$EPA_endtime_ind[is.na(pbp_data_epa$EPA_endtime_ind)] <- 0


# Assign EPA using these indicator columns: 
pbp_data_epa$EPA <- with(pbp_data_epa,
                         ifelse(EPA_off_td_ind == 1, EPA_off_td,
                                ifelse(EPA_off_fg_ind == 1, EPA_off_fg,
                                       ifelse(EPA_off_ep_ind == 1, EPA_off_ep,
                                              ifelse(EPA_off_tp_ind == 1, EPA_off_tp,
                                                     ifelse(EPA_PAT_fail_ind == 1, EPA_PAT_fail,
                                                            ifelse(EPA_safety_ind == 1, EPA_safety,
                                                                   ifelse(EPA_endtime_ind == 1, EPA_endtime,
                                                                          ifelse(EPA_change_score_ind == 1, EPA_change_score,
                                                                                 ifelse(EPA_change_no_score_nxt_ind == 1, EPA_change_no_score_nxt,
                                                                                        ifelse(EPA_base_nxt_ind == 1, EPA_base_nxt,
                                                                                               ifelse(EPA_change_no_score_ind == 1, EPA_change_no_score,
                                                                                                      EPA_base))))))))))))

# Assign PTDA using these indicator columns: 
pbp_data_epa$PTDA <- with(pbp_data_epa,
                          ifelse(EPA_off_td_ind == 1, PTDA_off_td,
                                 ifelse(EPA_off_fg_ind == 1, PTDA_off_fg,
                                        ifelse(EPA_off_ep_ind == 1, PTDA_off_ep,
                                               ifelse(EPA_off_tp_ind == 1, PTDA_off_tp,
                                                      ifelse(EPA_PAT_fail_ind == 1, PTDA_PAT_fail,
                                                             ifelse(EPA_safety_ind == 1, PTDA_safety,
                                                                    ifelse(EPA_endtime_ind == 1, PTDA_endtime,
                                                                           ifelse(EPA_change_score_ind == 1, PTDA_change_score,
                                                                                  ifelse(EPA_change_no_score_nxt_ind == 1, PTDA_change_no_score_nxt,
                                                                                         ifelse(EPA_base_nxt_ind == 1, PTDA_base_nxt,
                                                                                                ifelse(EPA_change_no_score_ind == 1, PTDA_change_no_score,
                                                                                                       PTDA_base))))))))))))

# Now drop the unnecessary columns
pbp_data_epa_final <- dplyr::select(pbp_data_epa, -c(EPA_base,EPA_base_nxt,
                                                     EPA_change_no_score,EPA_change_no_score_nxt,
                                                     EPA_change_score,EPA_off_td,EPA_off_fg,EPA_off_ep,
                                                     EPA_off_tp,EPA_safety,EPA_base_nxt_ind,
                                                     EPA_change_no_score_ind,EPA_change_no_score_nxt_ind,
                                                     EPA_change_score_ind,EPA_off_td_ind,EPA_off_fg_ind,EPA_off_ep_ind,
                                                     EPA_off_tp_ind,EPA_safety_ind, EPA_endtime_ind, EPA_endtime,
                                                     PTDA_base,PTDA_base_nxt,PTDA_PAT_fail,
                                                     PTDA_change_no_score,PTDA_change_no_score_nxt,
                                                     PTDA_change_score,PTDA_off_td,PTDA_off_fg,PTDA_off_ep,
                                                     PTDA_off_tp,PTDA_safety,PTDA_endtime, EPA_PAT_fail, EPA_PAT_fail_ind))


# Ungroup the dataset
pbp_data_epa_final <- dplyr::ungroup(pbp_data_epa_final)

#take out games with win margin > 49 points
pbp_data <- sqldf("select * from pbp_data_epa_final where gameID not in ('2016082601', '2016090200', '2017092200', '2017102000', '2017092300', '2016093001', '2016102900', '2017082500', '2017090100', '2017082400')");
 
pbp_data <- pbp_data %>%
  # Create an indicator for which half it is:
  mutate(Half_Ind = ifelse(qtr %in% c(1, 2), "Half1",
                           ifelse(qtr %in% c(3, 4), "Half2", "Overtime")),
         down = factor(down))


win_data <- sqldf("select a.GameID, case when PosTeamScore > DefTeamScore then posteam else DefensiveTeam end as Winner from pbp_data a inner join (select GameID, max(play_id) as max_play_id from pbp_data group by 1) b on a.GameID = b.GameID and a.play_id = b.max_play_id group by 1,2")

View(win_data)


pbp_wp_model_data <- pbp_data %>%
  mutate(GameID = as.character(GameID)) %>%
  left_join(win_data, by = "GameID") %>%
  # Create an indicator column if the posteam wins the game:
  mutate(Win_Indicator = ifelse(posteam == Winner, 1, 0),
         # Calculate the Expected Score Differential by taking the sum
         # fo the Expected Points for a play and the score differential:
         ExpScoreDiff = ExpPts + ScoreDiff,
         # Create a variable that is time remaining until end of half and game:
         TimeSecs_Remaining = ifelse(qtr %in% c(1,2), TimeSecs - 1440,
                                     ifelse(qtr == 5, TimeSecs + 900,
                                            TimeSecs)),
         # Under two-minute warning indicator
         Under_TwoMinute_Warning = ifelse(TimeSecs_Remaining < 120, 1, 0),
         # Define a form of the TimeSecs_Adj that just takes the original TimeSecs but
         # resets the overtime back to 900:
         TimeSecs_Adj = ifelse(qtr == 5, TimeSecs + 900, TimeSecs)) %>%
  # Remove the rows that don't have any ExpScoreDiff and missing Win Indicator:
  filter(!is.na(ExpScoreDiff) & !is.na(Win_Indicator) & play_type != "End of Game") %>%
  # Turn win indicator into a factor:
  mutate(Win_Indicator = as.factor(Win_Indicator),
         # Define a new variable, ratio of Expected Score Differential to TimeSecs_Adj
         # which is the same variable essentially as in Lock's random forest model
         # but now including the expected score differential instead:
         ExpScoreDiff_Time_Ratio = ExpScoreDiff / (TimeSecs_Adj + 1)) %>%
  # Remove overtime rows since overtime has special rules regarding the outcome:
  filter(qtr != 5) %>%
  # Turn the Half indicator into a factor:
  mutate(Half_Ind = as.factor(Half_Ind))

wp_model <- mgcv::bam(Win_Indicator ~ s(ExpScoreDiff) + 
                        s(TimeSecs_Remaining, by = Half_Ind) + 
                        s(ExpScoreDiff_Time_Ratio) + 
                        Under_TwoMinute_Warning*Half_Ind,
                      data = pbp_wp_model_data, family = "binomial")



# Initialize the vector to store the predicted win probability
# with respect to the possession team:
OffWinProb <- rep(NA, nrow(pbp_data_epa_final))

# Changing down into a factor variable
pbp_data_epa_final$down <- factor(pbp_data_epa_final$down)

# Get the Season and Month for rule changes:
pbp_data_epa_final <- dplyr::mutate(pbp_data_epa_final,
                                    Season = as.numeric(substr(as.character(GameID),1,4)),
                                    Month = as.numeric(substr(as.character(GameID),5,6)))


# Create a variable that is time remaining until end of half and game:
pbp_data_epa_final$TimeSecs_Remaining <- ifelse(pbp_data_epa_final$qtr %in% c(1,2),
                                                pbp_data_epa_final$TimeSecs - 1440,
                                                ifelse(pbp_data_epa_final$qtr == 5 & 
                                                         (pbp_data_epa_final$Season == 2017 & 
                                                            pbp_data_epa_final$Month > 4),
                                                       pbp_data_epa_final$TimeSecs + 600,
                                                       ifelse(pbp_data_epa_final$qtr == 5 &
                                                                (pbp_data_epa_final$Season < 2017 |
                                                                   (pbp_data_epa_final$Season == 2017 &
                                                                      pbp_data_epa_final$Month <= 4)),
                                                              pbp_data_epa_final$TimeSecs + 900,
                                                              pbp_data_epa_final$TimeSecs)))

# Expected Score Differential
pbp_data_epa_final <- dplyr::mutate(pbp_data_epa_final, ExpScoreDiff = ExpPts + ScoreDiff)


# Ratio of time to yard line
pbp_data_epa_final <- dplyr::mutate(pbp_data_epa_final,
                                    Time_Yard_Ratio = (1+TimeSecs_Remaining)/(1+yrdline100))

# Under Two Minute Warning Flag
pbp_data_epa_final$Under_TwoMinute_Warning <- ifelse(pbp_data_epa_final$TimeSecs_Remaining < 120,1,0)


# Define a form of the TimeSecs_Adj that just takes the original TimeSecs but
# resets the overtime back to 900 or 600 depending on year:

pbp_data_epa_final$TimeSecs_Adj <- ifelse(pbp_data_epa_final$qtr == 5 & 
                                            (pbp_data_epa_final$Season == 2017 & 
                                               pbp_data_epa_final$Month > 4),
                                          pbp_data_epa_final$TimeSecs + 600,
                                          ifelse(pbp_data_epa_final$qtr == 5 &
                                                   (pbp_data_epa_final$Season < 2017 |
                                                      (pbp_data_epa_final$Season == 2017 &
                                                         pbp_data_epa_final$Month <= 4)),
                                                 pbp_data_epa_final$TimeSecs + 900,
                                                 pbp_data_epa_final$TimeSecs))

# Define a new variable, ratio of Expected Score Differential to TimeSecs_Adj:

pbp_data_epa_final <- dplyr::mutate(pbp_data_epa_final,
                                    ExpScoreDiff_Time_Ratio = ExpScoreDiff / (TimeSecs_Adj + 1))

pbp_data_epa_final$Half_Ind <- with(pbp_data_epa_final,
                                    ifelse(qtr %in% c(1,2),"Half1","Half2"))
pbp_data_epa_final$Half_Ind <- as.factor(pbp_data_epa_final$Half_Ind)
OffWinProb <- as.numeric(mgcv::predict.bam(wp_model,newdata=pbp_data_epa_final,
                                           type = "response"))

DefWinProb <- 1 - OffWinProb

# Possession team Win_Prob

pbp_data_epa_final$Win_Prob <- OffWinProb

# Home: Pre-play

pbp_data_epa_final$Home_WP_pre <- ifelse(pbp_data_epa_final$posteam == pbp_data_epa_final$HomeTeam, OffWinProb, DefWinProb)

# Away: Pre-play

pbp_data_epa_final$Away_WP_pre <- ifelse(pbp_data_epa_final$posteam == pbp_data_epa_final$AwayTeam, OffWinProb, DefWinProb)

# Create the possible WPA values
pbp_data_epa_final <- dplyr::mutate(pbp_data_epa_final,
                                    # Team keeps possession (most general case):
                                    WPA_base = dplyr::lead(Win_Prob) - Win_Prob,
                                    # Team keeps possession but either Timeout, Two Minute Warning,
                                    # Quarter End is the following row:
                                    WPA_base_nxt = dplyr::lead(Win_Prob,2) - Win_Prob,
                                    # Change of possession and no timeout, 
                                    # two minute warning, or quarter end follows:
                                    WPA_change = (1 - dplyr::lead(Win_Prob)) - Win_Prob,
                                    # Change of possession but either Timeout,
                                    # Two Minute Warning, or
                                    # Quarter End is the following row:
                                    WPA_change_nxt = (1 - dplyr::lead(Win_Prob,2)) - Win_Prob,
                                    # End of quarter, half or end rows:
                                    WPA_halfend_to = 0)
# Create a WPA column for the last play of the game:
pbp_data_epa_final$WPA_final <- ifelse(dplyr::lead(pbp_data_epa_final$ScoreDiff) > 0 & dplyr::lead(pbp_data_epa_final$posteam) == pbp_data_epa_final$HomeTeam,
                                       1 - pbp_data_epa_final$Home_WP_pre,
                                       ifelse(dplyr::lead(pbp_data_epa_final$ScoreDiff) > 0 & dplyr::lead(pbp_data_epa_final$posteam) == pbp_data_epa_final$AwayTeam,
                                              1 - pbp_data_epa_final$Away_WP_pre,
                                              ifelse(dplyr::lead(pbp_data_epa_final$ScoreDiff) < 0 & dplyr::lead(pbp_data_epa_final$posteam) == pbp_data_epa_final$HomeTeam,
                                                     0 - pbp_data_epa_final$Home_WP_pre,
                                                     ifelse(dplyr::lead(pbp_data_epa_final$ScoreDiff) < 0 & dplyr::lead(pbp_data_epa_final$posteam) == pbp_data_epa_final$AwayTeam,
                                                            0 - pbp_data_epa_final$Away_WP_pre, 0))))


pbp_data_epa_final$WPA_base_nxt_ind <- with(pbp_data_epa_final, 
                                            ifelse(GameID == dplyr::lead(GameID) & 
                                                     posteam == dplyr::lead(posteam,2) &
                                                     Drive == dplyr::lead(Drive,2) & 
                                                     dplyr::lead(play_type) %in% 
                                                     c("Quarter End","Two Minute Warning","Timeout"),1,0))
pbp_data_epa_final$WPA_change_nxt_ind <- with(pbp_data_epa_final, 
                                              ifelse(GameID == dplyr::lead(GameID) & 
                                                       Drive != dplyr::lead(Drive,2) & 
                                                       posteam != dplyr::lead(posteam,2) &
                                                       dplyr::lead(play_type) %in% 
                                                       c("Quarter End","Two Minute Warning","Timeout"),1,0))
pbp_data_epa_final$WPA_change_ind <- with(pbp_data_epa_final,
                                          ifelse(GameID == dplyr::lead(GameID) & 
                                                   Drive != dplyr::lead(Drive) & 
                                                   posteam != dplyr::lead(posteam) &
                                                   dplyr::lead(play_type) %in% 
                                                   c("Pass","Run","Punt","Sack",
                                                     "Field Goal","No Play","QB Kneel",
                                                     "Spike","Kickoff"),1,0))
pbp_data_epa_final$WPA_halfend_to_ind <- with(pbp_data_epa_final, ifelse(play_type %in% c("Half End","Quarter End",
                                                                                          "End of Game","Timeout") | 
                                                                           (GameID == dplyr::lead(GameID) & sp != 1 &
                                                                              Touchdown != 1 & 
                                                                              is.na(FieldGoalResult) & 
                                                                              is.na(ExPointResult) & 
                                                                              is.na(TwoPointConv) & 
                                                                              Safety != 1 & 
                                                                              ((dplyr::lead(play_type) %in% 
                                                                                  c("Half End")) | 
                                                                                 (qtr == 2 & dplyr::lead(qtr)==3) | 
                                                                                 (qtr == 4 & dplyr::lead(qtr)==5))),1,0))
pbp_data_epa_final$WPA_final_ind <- with(pbp_data_epa_final, ifelse(dplyr::lead(play_type) %in% "End of Game", 1, 0))

# Replace the missings with 0 due to how ifelse treats missings
pbp_data_epa_final$WPA_base_nxt_ind[is.na(pbp_data_epa_final$WPA_base_nxt_ind)] <- 0
pbp_data_epa_final$WPA_change_nxt_ind[is.na(pbp_data_epa_final$WPA_change_nxt_ind)] <- 0
pbp_data_epa_final$WPA_change_ind[is.na(pbp_data_epa_final$WPA_change_ind)] <- 0 
pbp_data_epa_final$WPA_halfend_to_ind[is.na(pbp_data_epa_final$WPA_halfend_to_ind)] <- 0
pbp_data_epa_final$WPA_final_ind[is.na(pbp_data_epa_final$WPA_final_ind)] <- 0


# Assign WPA using these indicator columns: 
pbp_data_epa_final$WPA <- with(pbp_data_epa_final, 
                               ifelse(WPA_final_ind == 1,WPA_final,
                                      ifelse(WPA_halfend_to_ind == 1, WPA_halfend_to,
                                             ifelse(WPA_change_nxt_ind == 1, WPA_change_nxt,
                                                    ifelse(WPA_base_nxt_ind == 1, WPA_base_nxt,
                                                           ifelse(WPA_change_ind == 1, WPA_change,
                                                                  WPA_base))))))
# Home and Away post:

pbp_data_epa_final$Home_WP_post <- ifelse(pbp_data_epa_final$posteam == pbp_data_epa_final$HomeTeam,
                                          pbp_data_epa_final$Home_WP_pre + pbp_data_epa_final$WPA,
                                          pbp_data_epa_final$Home_WP_pre - pbp_data_epa_final$WPA)
pbp_data_epa_final$Away_WP_post <- ifelse(pbp_data_epa_final$posteam == pbp_data_epa_final$AwayTeam,
                                          pbp_data_epa_final$Away_WP_pre + pbp_data_epa_final$WPA,
                                          pbp_data_epa_final$Away_WP_pre - pbp_data_epa_final$WPA)

# For plays with play_type of End of Game, use the previous play's WP_post columns
# as the pre and post, since those are already set to be 1 and 0:
pbp_data_epa_final$Home_WP_pre <- with(pbp_data_epa_final,
                                       ifelse(play_type == "End of Game", dplyr::lag(Home_WP_post),
                                              Home_WP_pre))
pbp_data_epa_final$Home_WP_post <- with(pbp_data_epa_final,
                                        ifelse(play_type == "End of Game", dplyr::lag(Home_WP_post),
                                               Home_WP_post))
pbp_data_epa_final$Away_WP_pre <- with(pbp_data_epa_final,
                                       ifelse(play_type == "End of Game", dplyr::lag(Away_WP_post),
                                              Away_WP_pre))
pbp_data_epa_final$Away_WP_post <- with(pbp_data_epa_final,
                                        ifelse(play_type == "End of Game", dplyr::lag(Away_WP_post),
                                               Away_WP_post))


# Now drop the unnecessary columns
pbp_data_epa_final <- dplyr::select(pbp_data_epa_final, -c(WPA_base,WPA_base_nxt,WPA_change_nxt,WPA_change,
                                                           WPA_halfend_to, WPA_final,
                                                           WPA_base_nxt_ind, WPA_change_nxt_ind,
                                                           WPA_change_ind, WPA_halfend_to_ind, WPA_final_ind,
                                                           Half_Ind))

write.csv(pbp_data_epa_final,"pbp_data_epa_final.csv")