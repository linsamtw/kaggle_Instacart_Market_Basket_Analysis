
#install.packages("PerfMeas")

library(data.table)
library(xgboost)
library(glmnet)
library(dplyr)
#library(PerfMeas)
library(parallel)
library(tidyr)
library(mice)

setwd("/home/linsam/kaggle/kaggle_market_basket_analysis")

order_products__prior=fread("order_products__prior.csv")
order_products__train=fread("order_products__train.csv")
order_products = rbind(order_products__prior,order_products__train)

aisles = fread("aisles.csv")
departments = fread("departments.csv")
orders = fread("orders.csv")
products = fread("products.csv")
sample_submission= fread("sample_submission.csv")

orders$order_dow[orders$order_dow==0]=7
#--------------------------------------------------
# merge data
temp = work.train.test.data.fun(order_products,orders,products)

train=temp[[1]]
test =temp[[2]]
prior=temp[[3]]

#-----------------------------------------------
# fake test
part1.train = work.part1.data.fun(train)#8474661
#nrow(part1.train)
# really test
part1.test = work.part1.data.fun(test)#4833292
#nrow(part1.test)
data = rbind(part1.train,part1.test)

# rm(part1.train,part1.test)
gc()

#-----------------------------------------------
# feature engineering
data2 = feature_engineering_fun(prior,data,orders)
data3 = feature_engineering2_fun(prior,data2,orders,train,test)

# data2 = value
#rm(data)
gc()
nrow(data3)
#-------------------------------------------
# build model
# per=0.2
target =  data3$reordered[data3$eval_set=="train"]
# xgb model
xgb.model = build.model.fun(data3,per,seed=c(100))
pred.xgb = pred.fun(model=xgb.model,
                    data3,
                    model.name = "xgb",eval.name = "train")

xgb.fscore = compare.train.fscore(
  pred.xgb,per=0.2,
  y = target
  )
xgb.fscore
# 0.4415455
#****************************************
# glmnet model
glmnet.data = build.glmnet.data.fun(data3)
glmnet.model = build.glmnet.model.fun(glmnet.data)
pred.glmnet = pred.fun( model = glmnet.model, 
                        glmnet.data, 
                        model.name = "glmnet",eval.name = "train")

glmnet.fscore = compare.train.fscore(
  pred.glmnet,per=0.2,
  y = target
)
glmnet.fscore
# 0.2313076
#****************************************
# ensemble 
ensemble.pred = ensemble.pred.fun(pred.xgb,pred.glmnet,
           weight.xgb=0.8,weight.glmnet = 0.2)

ensemble.fscore = compare.train.fscore(
  ensemble.pred,per=0.18,
  y = target
)
ensemble.fscore
# 0.5:0.5  0.4358319
# 0.8:0.2  0.4412529
# 0.5:0.5, per = 0.15, 0.4087736
# 0.8:0.2, per = 0.18, 0.43669

work.importance.fun(data3,xgb.model)


#feature = feature.selection(data2,xgb.model2,n=48)


#??? add days_since_prior_order????????
#??? [50]	train-rmse:0.071017 
#??? [1] 0.9525831
#??? kaggle 0.2114328

# fix add days_since_prior_order
# [50]	train-rmse:0.264936 
# [1] 0.4414463

# add days_people_amount
# [50]	train-rmse:0.264958 
# [1] 0.4414611

# add dow_people_amount
# [50]	train-rmse:0.264947 
# [1] 0.4415224

# xxx add hour_of_day_order_amount
# xxx [50]	train-rmse:0.264970 
# xxx [1] 0.4416039
#********************************
# add hour_of_day_order_product_amount
# [50]	train-rmse:0.264952 
# [1] 0.4418253
# kaggle 0.3834591
#********************************
# add hour_of_day_order_product_per
# [50]	train-rmse:0.265003 
# [1] 0.4413042

#xxx add second_per
#xxx [50]	train-rmse:0.264973 
#xxx [1] 0.4415205

# add dow
# [50]	train-rmse:0.264941 
# [1] 0.4415875
#********************************
# dow 0->7
# [50]	train-rmse:0.264951 
# [1] 0.4415455
# kaggle 0.3835034
#********************************
# per = 0.15
# [50]	train-rmse:0.264951 
# [1] 0.422577
# kaggle 0.3750164

# ensemble
# xgb 0.4415455
# glm 0.2313076
# 0.5:0.5  0.4358319
# kaggle 0.3768664

# ensemble
# 0.8:0.2 0.4412529
# kaggle 0.3825349

# add return.rate
# [50]	train-rmse:0.264831 
# [1] 0.442105
# kaggle 0.3831690

#xxx days 30->NA
#xxx [50]	train-rmse:0.264812 
#xxx [1] 0.4419783

#********************************
# add order_days_hour , days=30
# [50]	train-rmse:0.264840 
# [1] 0.4421159
# kaggle 0.3836432
#********************************

# add order_days_hour , 30=NA
# [50]	train-rmse:0.264838 
# [1] 0.4421416
# kaggle 0.3833405

#xxx del return.rate
#xxx [50]	train-rmse:0.264949 
#xxx [1] 0.4416163
#xxx kaggle 0.3830928
#********************************
# fix return rate
# [50]	train-rmse:0.264770 
# [1] 0.4423715
# kaggle 0.3839605
#********************************
#xxx add user_ordeer_product_times, because the feature i already used
#xxx [50]	train-rmse:0.264780 
#xxx [1] 0.4424628
#xxx kaggle 0.3835829

# add user_buy_product_kinds
# [50]	train-rmse:0.264786 
# [1] 0.4424259
# kaggle 0.3835381

# add mean_user_product_cart
# [50]	train-rmse:0.264765 
# [1] 0.4426384

# add first_user_product_cart_times
# [50]	train-rmse:0.264722 
# [1] 0.4426157
#********************************
# add recent_user_product, rmse is down, but f-score is up
# [50]	train-rmse:0.264717 
# [1] 0.4425989
# kaggle 0.3841646
#********************************
#xxx log.loss
#xxx [50]	train-logloss:0.245323 
#xxx [1] 0.4418752
#xxx kaggle 0.3834083

#xxx fix days_since_prior_order
#xxx [50]	train-rmse:0.264733 
#xxx [1] 0.4421454

#xxx add second third
#xxx [50]	train-rmse:0.264795 
#xxx [1] 0.4425632
#********************************
# add mean_user_order_amount
# [50]	train-rmse:0.264638 
# [1] 0.4431728
# kaggle 0.3837617

# add user_next_order_product_amount
# [50]	train-rmse:0.264703 
# [1] 0.4430125
# kaggle 0.3850201
#********************************
#xxx del mean_user_order_amount
#xxx [50]	train-rmse:0.264645 
#xxx [1] 0.4429675
#xxx kaggle 0.3844104



#-------------------------------------------
# final prediction
final.pred = 
  final.pred.fun(per=0.2,
                 xgb.model,glmnet.model,
                 data3,glmnet.data,
                 weight.xgb=1,weight.glmnet = 0)

final.pred[1:5,]

fwrite(final.pred,"pred.csv")






