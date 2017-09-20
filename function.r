
# function

work.train.test.data.fun=function(order_products,orders,products){
  
  products$group = paste(products$department_id,products$aisle_id,sep="-")
  
  temp = 	merge(products,order_products,all=T,by=c("product_id")) %>%
    merge(.,orders,all=T,by=c("order_id")) %>%
    arrange(order_id) %>% data.table
  
  temp$product_name=NULL

  temp = temp[,.(order_id,
                 user_id,
                 eval_set,
                 order_number,
                 order_dow,
                 order_hour_of_day,
                 days_since_prior_order,
                 aisle_id,
                 department_id,
                 product_id,
                 group,
                 add_to_cart_order,
                 reordered)]
  
  test  = filter(temp,eval_set=="test") %>% 
    arrange(user_id) %>% 
    data.table
  train = filter(temp,eval_set=="train") %>% 
    arrange(user_id) %>% 
    data.table
  prior = filter(temp,eval_set=="prior") %>% 
    arrange(user_id) %>% 
    data.table
  
  rm(temp)
  gc()
  
  return(list(train,test,prior))
}

# find kinds of user-product 
work.part1.data.fun=function(train){
  
  train.prior = 
    prior %>% 
    filter(.,user_id %in% train$user_id) %>%
    data.table
  
  temp = 
    train.prior %>% 
    subset(.,select = c(user_id,
                        product_id
    ))

  temp$name = paste(temp$user_id,temp$product_id,sep="-")
  name = unique(temp$name)
  tem1 = strsplit(name,split="-")
  value = do.call(rbind,tem1) %>% data.table
  colnames(value)=c("user_id","product_id")
  value$user_id = as.integer(value$user_id)
  value$product_id = as.integer(value$product_id)

  #value$order_dow = as.integer(value$order_dow)
  #str(value)
  #-------------------------------------------------
  value = 
    train[,.(order_dow=unique(order_dow)),by=c("user_id")] %>%
    merge(value,.,by=c("user_id"))
    
  value = 
    train[,.(order_hour_of_day=unique(order_hour_of_day)),
          by=c("user_id")] %>%
    merge(value,.,by=c("user_id")) 
  
  value = 
    train[,.(days_since_prior_order=unique(days_since_prior_order)),
          by=c("user_id")] %>%
    merge(value,.,by=c("user_id")) 
  #-------------------------------------------------
  cv.data = 
    train %>% 
    subset(.,select = c(user_id,
                        product_id,
                        reordered
    ))
  #value
  value2 = 
    merge(value,cv.data,all=T,
          by=c("user_id","product_id"))
  #table(value2$eval_set)
  # we don't predict reodered==0, that is new order product
  # that will be part2 
  value3 = filter(value2,is.na(reordered)) %>% data.table
  value4 = filter(value2,reordered==1) %>% data.table
  value5 = rbind(value3,value4) %>% arrange(user_id,product_id) %>% data.table
  
  value5$eval_set =  unique(train$eval_set)
  value5$reordered[is.na(value5$reordered)] = 0
  value5 = value5[!is.na(product_id)]
  #gc()
  return(value5)
}

work.product.feature = function(prior){
  #----------------------------------------------
  #----------------------------------------------
  # product order amount
  product_order_amount = 
    prior[,.(product_order_amount = length(eval_set)),
          by=c("product_id")] %>% 
    arrange(product_id) %>% data.table
  
  # product reorder amount
  product_reordered_amount = 
    prior[,.(product_reordered_amount = sum(reordered)),
          by=c("product_id")] %>% 
    arrange(product_id) %>% data.table
  
  # product first order amount ( first is reordered==0 )
  product_first_order_amount = 
    prior[,.(product_first_order_amount = 
               sum(reordered==0)),
          by=c("product_id")] %>% 
    arrange(product_id) %>% data.table  
  
  # prod_reorder_probability                      
  product_reorder_prob = 
    prior[,.(product_reorder_prob = 
               sum(reordered)/length(reordered) ),
          by=c("product_id")] %>% 
    arrange(product_id) %>% 
    data.table
  
  return( list(product_order_amount,
               product_reordered_amount,
               product_first_order_amount,
               product_reorder_prob
               ) )
}

work.aisle.department.feature = function(prior){
  aisle_department = 
    prior %>% subset(select = 
                       c("user_id","product_id",
                         "aisle_id","department_id"))
  aisle_amount = 
    aisle_department[,.(
      aisle_amount = length(unique(aisle_id)))
      ,by=c("user_id")]

  department_amount = 
    aisle_department[,.(
      department_amount = length(unique(department_id)))
      ,by=c("user_id")]

  user_amount = n_distinct(aisle_department$user_id)
  #--------------------------------------------------------
  department_martix = 
    rep(0,nrow(departments)) %>% 
    matrix(.,ncol=nrow(departments),
           dimnames = 
             list(c(1),
                  departments$department)) %>% data.table

  temp = mclapply(c(1:user_amount),function(i){#user_amount
    
    x = aisle_department[user_id==i] # i=1
    x2 = unique(x$department_id)
  
    department_martix[,x2]=1
    department_martix = data.table(user_id=i,department_martix)
  
    return(department_martix) 
    
    },mc.cores=8, mc.preschedule = T)
 
  department_martix = do.call(rbind,temp)
  rm(temp)
  gc()
  #--------------------------------------------------------
  #aisle_martix = 
  #  rep(0,nrow(aisles)) %>% 
  #  matrix(.,ncol=nrow(aisles),
  #         dimnames = 
  #           list(c(1),
  #                aisles$aisle)) %>% data.table
  
  #aisle_martix = mclapply(c(1:user_amount),function(i){#
  #  x = aisle_department[user_id==i] # i=1
  #  x2 = unique(x$aisle_id)
  #  aisle_martix[,x2]=1
  #  aisle_martix = data.table(user_id=i,aisle_martix)
  #  return(aisle_martix) 
  #},mc.cores=8, mc.preschedule = T) %>% do.call(rbind,.)
  
  #aisles
  #departments
  
  return(list(aisle_amount,
              department_amount
              ,department_martix
              #aisle_martix
              ))
}

work_last_order_product_fun=function(prior){
  
  # last_order_product
  temp = 
    prior[,.(last_order_product = 
               max(order_number)),
          by=c("user_id")]
  temp2 = prior %>% 
    subset(select=c(user_id,product_id,order_number)) %>%
    merge(.,temp,by=c("user_id"))
  
  last_order_product = temp2[order_number == last_order_product]
  last_order_product$last_order_product=1
  
  last_order_product = 
    subset(last_order_product,
           select = c("user_id","product_id","last_order_product"))
  
  # last2_order_product
  temp2$last_order_product=temp2$last_order_product-1
  
  last2_order_product = temp2[order_number == last_order_product]
  last2_order_product$last2_order_product=1
  
  last2_order_product = 
    subset(last2_order_product,
           select = c("user_id","product_id","last2_order_product"))
  
  # last3_order_product
  temp2$last_order_product=temp2$last_order_product-1
  
  last3_order_product = temp2[order_number == last_order_product]
  last3_order_product$last3_order_product=1
  
  last3_order_product = 
    subset(last3_order_product,
           select = c("user_id","product_id","last3_order_product"))
  
  # last4_order_product
  temp2$last_order_product=temp2$last_order_product-1
  
  last4_order_product = temp2[order_number == last_order_product]
  last4_order_product$last4_order_product=1
  
  last4_order_product = 
    subset(last4_order_product,
           select = c("user_id","product_id","last4_order_product"))  
  
  # last5_order_product
  temp2$last_order_product=temp2$last_order_product-1
  
  last5_order_product = temp2[order_number == last_order_product]
  last5_order_product$last5_order_product=1
  
  last5_order_product = 
    subset(last5_order_product,
           select = c("user_id","product_id","last5_order_product"))  
  
  # last6_order_product
  temp2$last_order_product=temp2$last_order_product-1
  
  last6_order_product = temp2[order_number == last_order_product]
  last6_order_product$last6_order_product=1
  
  last6_order_product = 
    subset(last6_order_product,
           select = c("user_id","product_id","last6_order_product"))    

  # last7_order_product
  temp2$last_order_product=temp2$last_order_product-1
  
  last7_order_product = temp2[order_number == last_order_product]
  last7_order_product$last7_order_product=1
  
  last7_order_product = 
    subset(last7_order_product,
           select = c("user_id","product_id","last7_order_product"))   
  # last8_order_product
  temp2$last_order_product=temp2$last_order_product-1
  
  last8_order_product = temp2[order_number == last_order_product]
  last8_order_product$last8_order_product=1
  
  last8_order_product = 
    subset(last8_order_product,
           select = c("user_id","product_id","last8_order_product"))   
  # last9_order_product
  temp2$last_order_product=temp2$last_order_product-1
  
  last9_order_product = temp2[order_number == last_order_product]
  last9_order_product$last9_order_product=1
  
  last9_order_product = 
    subset(last9_order_product,
           select = c("user_id","product_id","last9_order_product"))
  
  # last10_order_product
  #temp2$last_order_product=temp2$last_order_product-1
  
  #last10_order_product = temp2[order_number == last_order_product]
  #last10_order_product$last10_order_product=1
  
  #last10_order_product = 
  #  subset(last10_order_product,
  #         select = c("user_id","product_id","last10_order_product"))   
  # last11_order_product
  #temp2$last_order_product=temp2$last_order_product-1
  
  #last11_order_product = temp2[order_number == last_order_product]
  #last11_order_product$last11_order_product=1
  
  #last11_order_product = 
  #  subset(last11_order_product,
  #         select = c("user_id","product_id","last11_order_product"))   
  # last12_order_product
  #temp2$last_order_product=temp2$last_order_product-1
  
  #last12_order_product = temp2[order_number == last_order_product]
  #last12_order_product$last12_order_product=1
  
  #last12_order_product = 
  #  subset(last12_order_product,
  #         select = c("user_id","product_id","last12_order_product"))   
  return(list(
    last_order_product,
    last2_order_product,
    last3_order_product,
    last4_order_product,
    last5_order_product,
    last6_order_product,
    last7_order_product,
    last8_order_product,
    last9_order_product#,
    #last10_order_product,
    #last11_order_product,
    #last12_order_product
  ))
}


feature_engineering_fun=function(prior,data,orders){
  
  #----------------------------------------------
    # user total order amount
  user_order_amount = prior[,.(order_amount = n_distinct(order_id)),
                by=.(user_id)]
  
  # user order product amount
  user_product_amount = prior[,.(product_amount = length(order_id)),
                by=.(user_id,product_id)]  %>% 
    arrange(user_id,product_id) %>% 
    data.table
  
  # user buy product precent
  user_buy_product_percent = 
    merge(user_order_amount,user_product_amount,by=c("user_id"))[
      ,.(user_buy_product_percent = 
           product_amount/order_amount),
      by=.(user_id,product_id)]

  #----------------------------------------------
  # meae of order amount
  mean_order_amount = 
    prior[,.(order_amount = length( product_id )),
                by=c("user_id","order_id")
                ][,.(mean_order_amount = 
                       mean(order_amount)),by=.(user_id)] 
  
  mean_order_hour_of_day = 
    prior[,.(mean_order_hour_of_day = 
                     mean( order_hour_of_day )),
                by=c("user_id","product_id")]
  max_order_hour_of_day = 
    prior[,.(max_order_hour_of_day = 
                     max( order_hour_of_day )),
                by=c("user_id","product_id")]
  min_order_hour_of_day = 
    prior[,.(min_order_hour_of_day = 
                     min( order_hour_of_day )),
                by=c("user_id","product_id")]

  #----------------------------------------------
  # first order per
  temp = 
    prior %>% 
    arrange(user_id,order_number,add_to_cart_order) %>% 
    filter(add_to_cart_order==1) %>%
    data.table %>% 
    subset(select = c(user_id,order_id,
                      product_id))
  order_amount = 
    temp[,.(order_amount = length(order_id)),by=c("user_id")]
  proudct_amount = 
    temp[,.(proudct_amount = length(order_id)),by=c("user_id","product_id")]
  
  temp2 = merge(proudct_amount,order_amount,by=c("user_id"))
  
  first_per = temp2[,.(first_per = proudct_amount/order_amount),
     by=c("user_id","product_id")]

  # first order 0 1 
  temp = 
    prior %>% 
    arrange(user_id,order_number,add_to_cart_order) %>% 
    filter(add_to_cart_order==1) %>%
    data.table %>% 
    subset(select = c(user_id,order_id,
                      product_id))

  first.order = temp[,.(product_id = unique(product_id)),by=c("user_id")]
  first.order$first.order = 1
  
  #----------------------------------------------
  # work product feature
  temp = work.product.feature(prior)
  product_order_amount = temp[[1]]
  product_reordered_amount = temp[[2]]
  product_first_order_amount = temp[[3]]
  product_reorder_prob = temp[[4]]
  #----------------------------------------------
  # work aisle department feature
  temp = work.aisle.department.feature(prior)
  aisle_amount = temp[[1]]
  department_amount = temp[[2]]
  department_martix = temp[[3]]
  #aisle_martix = temp[[4]]
  #----------------------------------------------
  # orders
  # user order day of week class
  user_order_dow_times = 
    prior[,.(user_order_dow_times = length(unique(order_dow)))
          ,by=c("user_id")]

  # last order amount
  last_order_amount = 
    prior[,.(last_order_amount = 
               sum(max(order_number)==order_number)
             ),by=c("user_id")]
  
  # mean_days_since_prior_order
  mean_days_since_prior_order = 
    orders[eval_set=="prior",.(mean_days_since_prior_order = 
                mean(days_since_prior_order,na.rm=T)),
           by=c("user_id")]
  
  # last_order_product is important
  temp = work_last_order_product_fun(prior)
  last_order_product  = temp[[1]]
  last2_order_product = temp[[2]]
  last3_order_product = temp[[3]]
  last4_order_product = temp[[4]]
  last5_order_product = temp[[5]]
  last6_order_product = temp[[6]]
  last7_order_product = temp[[7]]
  last8_order_product = temp[[8]]
  last9_order_product = temp[[9]]

  #----------------------------------------------
  # mean order_dow
  mean_order_dow = 
    prior[,.(mean_order_dow = mean(order_dow)),
          by=c("user_id","product_id")]
  
  # last order_dow
  temp = 
    prior[,.(last_order_product = 
               max(order_number)),
          by=c("user_id")]
  temp2 = prior %>% 
    subset(select=c(user_id,order_number,order_dow)) %>%
    merge(.,temp,by=c("user_id"))
  
  temp3 = temp2[order_number==last_order_product]
  
  last_order_dow = 
    temp3[,.(last_order_dow = unique(order_dow)),
          by=c("user_id")]
  
  #----------------------------------------------
  # mean_product_order_hour
  
  # mean_product_order_dow
  
  # mean_user_product_order_hour
  mean_product_order_hour = 
    prior[,.(mean_product_order_hour = mean(order_hour_of_day)),
          by=c("product_id")]
  
  # mean_user_product_order_dow
  
  # mean_product_days_since_prior_order
  mean_product_days_since_prior_order = 
    prior[,.(mean_product_days_since_prior_order
             =mean(days_since_prior_order,na.rm=T)),
          by=c("product_id")]
  
  #----------------------------------------------
  temp1 = prior %>% 
    subset(select=c("user_id","order_id","order_hour_of_day"
                    ))
  temp2 = train %>% 
    subset(select=c("user_id","order_id","order_hour_of_day"
    ))
  temp3 = test %>% 
    subset(select=c("user_id","order_id","order_hour_of_day"
    ))
  #prior
  #data
  # order_dow 
  # order_hour_of_day
  #----------------------------------------------
  # order_product_dow_per
  # order_dow 
  # the point is merge user_id & product_id, but i need order_dow
  
  # mean order_dow product per
  #temp3 = prior[,.(product_order_dow_amount = length(order_id)),
  #              by=c("order_dow","product_id")]
  #temp4 = temp3[,.(product_amount=sum(product_order_dow_amount)),
  #              by=c("product_id")]
  
  #temp5 = merge(temp3,temp4,by=c("product_id"))
  
  #order_product_dow_per = 
  #  temp5[,.(order_product_dow_per = product_order_dow_amount/product_amount),
  #        by=c("product_id","order_dow")] 
  
  # user_order_product_dow_per
  # ex: user_id == 1, product_id == 196, he buy 10 times, and
  # he buy 196 on order_dow == 4 is 3
  # so he buy_per is 0.3
  #temp6 = prior[,.(product_order_dow_amount = length(order_id)),
  #              by=c("order_dow","product_id","user_id")]
  
  #temp6[user_id==1]
  
  #temp7 = temp6[,.(product_amount=sum(product_order_dow_amount)),
  #              by=c("product_id","user_id")]
  #temp8 = merge(temp6,temp7,by=c("product_id","user_id")) 
  
  #user_order_product_dow_per = 
  #  temp8[,.(user_order_product_dow_per = 
  #             product_order_dow_amount/product_amount),
  #        by=c("user_id","product_id","order_dow")] %>%
  #  arrange(user_id,product_id) %>% 
  #  data.table
  # note user_order_product_dow_per merge data2 will be na too much
  
  #user_mean_order_dow_amount

  
  #----------------------------------------------
  #----------------------------------------------
  #----------------------------------------------
  #----------------------------------------------
  #----------------------------------------------
  data2 = data %>%
    merge(.,user_order_amount,all.x=T,by=c("user_id")) %>%
    merge(.,user_product_amount,all.x=T,
          by=c("user_id","product_id")) %>%
    merge(.,user_buy_product_percent,all.x=T,
          by=c("user_id","product_id")) %>%
    merge(.,mean_order_hour_of_day,all.x=T,
          by=c("user_id","product_id")) %>%
    merge(.,max_order_hour_of_day,all.x=T,
          by=c("user_id","product_id")) %>%
    merge(.,min_order_hour_of_day,all.x=T,
          by=c("user_id","product_id")) %>%
    merge(.,first_per,all.x=T,
          by=c("user_id","product_id")) %>%
    merge(.,first.order,all.x=T,
          by=c("user_id","product_id")) %>%
    merge(.,product_order_amount,all.x=T,
         by=c("product_id")) %>%
    merge(.,product_reordered_amount,all.x=T,
          by=c("product_id")) %>%
    merge(.,product_first_order_amount,all.x=T,
          by=c("product_id")) %>%
    merge(.,aisle_amount,all.x=T,
          by=c("user_id")) %>%
    merge(.,department_amount,all.x=T,
          by=c("user_id")) %>%
    merge(.,department_martix,all.x=T,by=c("user_id")) %>%
    merge(.,product_reorder_prob,all.x=T,
          by=c("product_id")) %>%
    #merge(.,user_order_dow_times,all.x=T,by=c("user_id")) %>% 
    merge(.,last_order_amount,all.x=T,by=c("user_id")) %>%
    merge(.,last_order_product,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,last2_order_product,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,last3_order_product,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,last4_order_product,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,last5_order_product,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,last6_order_product,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,last7_order_product,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,last8_order_product,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,last9_order_product,all.x=T,by=c("user_id","product_id")) #%>%
    #merge(.,order_product_dow_per,all.x=T,by=c("product_id","order_dow"))

  return(data2)
}

feature_engineering2_fun=function(prior,data2,orders,train,test){
  
  #----------------------------------------------
  #temp = prior[,.(user_product_last_order_number = 
  #                  max(order_number)),by=c("user_id","product_id")]
  #temp2 = prior[,.(user_last_order_number = max(order_number)),by=c("user_id")]
  
  #temp3 = merge(temp,temp2,by=c("user_id")) %>% 
  #  arrange(user_id,product_id) %>% 
  #  data.table
  
  #temp3$user_product_recency_order_number = 
  #  temp3$user_last_order_number-temp3$user_product_last_order_number+1
  
  #user_product_recency_order_number = 
  #  temp3 %>% 
  #  select(user_id,product_id,user_product_recency_order_number)
  
  #-----------------------------------------------------------
  tem2 = prior[,.(days_since_prior_order = days_since_prior_order),
               by=c("user_id","order_id")] 
  tem3 = tem2[,.(unique = unique(days_since_prior_order)),
              by=c("user_id","order_id")]
  user_total_days_since_orior_order = 
    tem3[,.(user_total_days_since_orior_order = sum(unique,na.rm=T)),
         by=c("user_id")]
  print(1)
  #---------------------------------------------------------
  product_reordered_amount = 
    prior[,.(product_reordered_amount = sum(reordered)),by=c("product_id")]
  
  product_reordered_per = 
    prior[,.(product_reordered_per = sum(reordered)/length(reordered)),
          by=c("product_id")] %>% 
    arrange(product_id) %>%
    data.table
  #---------------------------------------------------------
  mean_order_product_days = 
    prior[,.(mean_order_product_days = mean(days_since_prior_order,na.rm=T)),
          by=c("product_id")]
  print(2)
  #------------------------------------------------
  temp.prior = prior %>% arrange(order_number) %>% data.table
  
  temp1 = temp.prior[,.(order_number = max(order_number)),by=c("user_id","product_id")]
  
  temp2.prior = 
    temp.prior[,.(days_since_prior_order = unique(days_since_prior_order)),
                       by=c("user_id","order_number")] %>%
    arrange(user_id,desc(order_number)) %>% data.table
  
  temp2.prior$days_since_prior_order[is.na(temp2.prior$days_since_prior_order)]=5
  n = length( unique(temp2.prior$user_id) )

  temp2.prior$recent_user_order_product_days = 
    mclapply( c( 1 : n ),function(i){
      x = temp2.prior[user_id==i]
      value = cumsum( x$days_since_prior_order )

      return(value)
    },mc.cores=8, mc.preschedule = T) %>% do.call(c,.)

  days_since_prior_order = 
    merge(temp2.prior,temp1,all.y=T,by=c("order_number","user_id")) %>%
    subset(select = c("user_id","product_id","recent_user_order_product_days"))
  print(3)
  #------------------------------------------------
  days_people_amount = prior[,.(days_people_amount = length(unique(user_id))),by=c("days_since_prior_order")]
  dow_people_amount = prior[,.(dow_people_amount = length(unique(user_id))),by=c("order_dow")]
  
  hour_of_day_order_amount = 
    prior[,.(hour_of_day_order_amount = length(product_id)),
          by=c("order_hour_of_day")]
  
  hour_of_day_order_product_amount = 
    prior[,.(hour_of_day_order_product_amount = length(order_id)),
          by=c("order_hour_of_day","product_id")]
  print(4)
  #----------------------------------------------
  #product_dow_order_per = prior[,.(),]
  
  tem = merge(hour_of_day_order_product_amount,
              hour_of_day_order_amount,by=c("order_hour_of_day"))
  
  hour_of_day_order_product_per = 
    tem[,.(hour_of_day_order_product_per = 
           hour_of_day_order_product_amount/hour_of_day_order_amount),
      by=c("order_hour_of_day","product_id")]
  print(5)
  #----------------------------------------------
  # second order per
  #temp = 
  #  prior %>% 
  #  arrange(user_id,order_number,add_to_cart_order) %>% 
  #  filter(add_to_cart_order==2) %>%
  #  data.table %>% 
  #  subset(select = c(user_id,order_id,
  #                    product_id))
  #order_amount = 
  #  temp[,.(order_amount = length(order_id)),by=c("user_id")]
  #proudct_amount = 
  #  temp[,.(proudct_amount = length(order_id)),by=c("user_id","product_id")]
  
  #temp2 = merge(proudct_amount,order_amount,by=c("user_id"))
  
  #second_per = temp2[,.(second_per = proudct_amount/order_amount),
  #                   by=c("user_id","product_id")]
  
  #data2$order_dow[data2$order_dow==0] = 7
  
  # 
  #popular_product_matr = Reduce( function(x,y) merge(x,y),
  #                               list(popular_product_1,
  #                                    popular_product_2,
  #                                    popular_product_3,
  #                                    popular_product_4,
  #                                    popular_product_5))
  #------------------------------------------------
  # return rate
  product_id.name = unique(prior$product_id)
  
  return.rate = 
    mclapply(c(1:length(product_id.name)), # 
             function(k){
               x = prior[product_id==product_id.name[k]] %>% 
                 subset(select=c("user_id","product_id",
                                 "order_number","reordered")) %>%
                 arrange(user_id,order_number) %>% data.table
               
               x2 = x[,.(list(order_number)),by=c("user_id")]
               
               x3 = sapply(c(1:nrow(x2)),function(i){ # i=1
                 return.fun(x2$V1[[i]])
               })
               
               value = data.table( product_id = product_id.name[k] ,  
                                   return.rate = sum(x3,na.rm=T)/length(x3)) 
               return(value)
             },mc.cores=16, mc.preschedule = T) %>% 
    do.call(rbind,.);gc()

  
  
  #first = prior[order_number==1] %>% 
  #  subset(select=c("user_id","product_id","reordered"))
  #colnames(first) = c("user_id","product_id","first")
  #second = prior[order_number==2] %>% 
  #  subset(select=c("user_id","product_id","reordered"))
  #colnames(second) = c("user_id","product_id","second")
  
  #tem = merge(first,second,all.x=T,by=c("user_id","product_id"))
  
  #return.rate = 
  #  tem[,.(return.rate = sum(second,na.rm=T)/length(second)),
  #      by=c("product_id")]
  
  #length( tem$second[tem$product_id==196])
  
  #------------------------------------------------
  #tem = rbind(train,test)
  order_days_hour = 
    data2 %>% subset(select=c("user_id","product_id"))
  order_days_hour$order_days_hour = 
    data2$order_dow*24+data2$order_hour_of_day
  #------------------------------------------------
  #user_ordeer_product_times = 
  #  prior[,.(user_ordeer_product_times = 
  #             length(order_number)),
  #        by=c("user_id","product_id")]
  
  #user_buy_product_kinds = 
  #  prior[,.(user_buy_product_kinds = n_distinct(product_id) ),
  #        by=c("user_id")]
  mean_user_product_cart =
    prior[,.(mean_user_product_cart = 
               mean(add_to_cart_order)),
          by=c("user_id","product_id")]
  first_user_product_cart_times = 
    prior[,.(first_user_product_cart_times =
               sum(add_to_cart_order==1) ),
          by=c("user_id","product_id")]
  #second_user_product_cart_times = 
  #  prior[,.(second_user_product_cart_times =
  #             sum(add_to_cart_order==2) ),
  #        by=c("user_id","product_id")]
  #third_user_product_cart_times = 
  #  prior[,.(third_user_product_cart_times =
  #             sum(add_to_cart_order==3) ),
  #        by=c("user_id","product_id")]
  #------------------------------------------------
  tem = 
    prior[,.(x = max(add_to_cart_order)),
          by=c("user_id","order_id")]
  mean_user_order_amount = 
    tem[,.(mean_user_order_amount = mean(x)),
        by=c("user_id")]
  #------------------------------------------------
  #------------------------------------------------
  #------------------------------------------------
  #------------------------------------------------
  
  data3 = data2 %>% 
    merge(.,user_total_days_since_orior_order,all.x=T,by=c("user_id")) %>%
    merge(.,mean_order_product_days,all.x=T,by=c("product_id")) %>%
    merge(.,days_since_prior_order,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,days_people_amount,all.x=T,by=c("days_since_prior_order")) %>%
    merge(.,dow_people_amount,all.x=T,by=c("order_dow")) %>%
    merge(.,hour_of_day_order_product_amount,all.x=T,
          by=c("order_hour_of_day","product_id")) %>%
    merge(.,return.rate,all.x=T,by=c("product_id")) %>%
    merge(.,order_days_hour,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,mean_user_product_cart,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,first_user_product_cart_times,all.x=T,by=c("user_id","product_id")) %>%
    merge(.,mean_user_order_amount,all.x=T,by=c("user_id")) 
  
  data3$recent_user_product = 
    data3$mean_order_product_days - data3$days_since_prior_order
  
  data3$user_next_order_product_amount = 
    data3$mean_user_product_cart-data3$mean_user_order_amount

    #merge(.,dow_people_amount,all.x=T,by=c("order_dow"))
 #data3$days_since_prior_order[data3$days_since_prior_order==30]=NA
  
  return(data3)
}


return.fun=function(v=c(1)){
  if(length(v)==1) return(NA)
  v2 = v[2]-v[1]
  if(v2==1){
    return(1)
  }else if(v2!=1){
    return(0)
  }
}

build.glmnet.data.fun=function(data3){
  glmnet.data = data3
  #------------------------------------------------
  #------------------------------------------------
  glmnet.data$first_per[is.na(glmnet.data$first_per)]=0
  glmnet.data$first.order[is.na(glmnet.data$first.order)]=0
  glmnet.data$last_order_product[is.na(glmnet.data$last_order_product)]=0
  glmnet.data$last2_order_product[is.na(glmnet.data$last2_order_product)]=0
  glmnet.data$last3_order_product[is.na(glmnet.data$last3_order_product)]=0
  glmnet.data$last4_order_product[is.na(glmnet.data$last4_order_product)]=0
  glmnet.data$last5_order_product[is.na(glmnet.data$last5_order_product)]=0
  glmnet.data$last6_order_product[is.na(glmnet.data$last6_order_product)]=0
  glmnet.data$last7_order_product[is.na(glmnet.data$last7_order_product)]=0
  glmnet.data$last8_order_product[is.na(glmnet.data$last8_order_product)]=0
  glmnet.data$last9_order_product[is.na(glmnet.data$last9_order_product)]=0
  
  map = find.na.fun(glmnet.data)
  
  glmnet.data = glmnet.data[,-c(map),with=F]
  #x = data4$hour_of_day_order_product_amount 
  #x = data.table(x)
  #set.seed(100)
  #fix.x = complete(x,m=5,)
  gc()
  return(glmnet.data)
}

build.glmnet.model.fun=function(glmnet.data){
  
  #------------------------------------------------
  x = glmnet.data %>% 
    filter(eval_set=="train") %>%
    data.table %>% 
    subset(select = -c(user_id,product_id,
                       reordered,eval_set
                       #,order_dow
                       #,order_hour_of_day
    ))
  
  y = glmnet.data %>% 
    filter(eval_set=="train") %>%
    data.table %>% 
    subset(select = c(reordered))
  y = y$reordered
  
  set.seed(100)
  glmnet.model = glmnet(as.matrix(x),(y),
                        family = c("poisson")
                        ,alpha = 0.5
                        ,nlambda = 6
                        ,standardize = FALSE
                        ,maxit=100000)
  plot(glmnet.model)
  return(glmnet.model)
}
  
  
find.na.fun=function(data4){
  value = 
    sapply(c(1:ncol(data4)),function(i){
      #print(i)
      value =  sum(is.na(data4[,i,with=F]) )
      if(value !=0){
        return(i)
      }else{return()}
    })
  
  value2 = unique(do.call(c,value)) 
  return(value2)
}

f_score=function(train.table){
  TP = train.table[2,2]
  FP = train.table[2,1]
  FN = train.table[1,2]
  
  precision = TP/(TP+FP)
  recall = TP/(TP+FN)
  
  #f_score = 2*TP/(2*TP+FP+FN)
  f_score = 2*(precision*recall/(precision+recall))
  return(f_score)
}

ensemble.pred.fun = function(model,dtrain){
  # model = xgb.model
  pred = sapply(c(1:length(model)),
                function(i){
                  value = predict(model[[i]],dtrain)
                  return(value)
                }
  ) %>% data.table
  
  ensemble.pred =  mclapply(c(1:nrow(pred)), # 
                            function(i){
                              value = pred[i,] %>% as.matrix %>% mean
                              return(value)
                            },mc.cores=16, mc.preschedule = T)
  ensemble.pred = do.call(c,ensemble.pred)
  return(ensemble.pred)  
}


work_user_buy_product_order_dow_fun=function(train.prior){
  


  return(temp4)
}

build.model.fun=function(data3,per=0.2,seed=c(100)){
  
  subtrain = data3 %>%
    filter(eval_set=="train") %>% data.table
  
  subtrain.x = subtrain %>% 
    subset(select = -c(user_id,product_id,
                       reordered,eval_set
                       #,order_dow
                       #,order_hour_of_day
                       ))
  ncol(subtrain.x)
  
  subtrain.y = subtrain %>% 
    subset(select = c(reordered))
  
  dtrain = xgb.DMatrix(data = as.matrix(subtrain.x),
                       label = subtrain.y$reordered)
  #rm(subtrain,subtrain.x)

  # build model
  if(length(seed)!=1){
    model = sapply( c(1:length(seed)),function(i) 
      xgb.fun2(dtrain,seed[i])
      )
    # 6min
    pred.train = ensemble.pred.fun(model,dtrain)
  }else if(length(seed)==1){ # 
    model = xgb.fun(dtrain,seed)
    #pred.train = predict(model,dtrain)
  }
  return((model))
}
  

pred.fun=
  function(model=xgb.model,
           data3,model.name = "xgb", 
           eval.name = "train"){
  
  if(model.name=="xgb"){
    subtrain = data3 %>%
      filter( eval_set == eval.name ) %>% data.table
    
    subtrain.x = subtrain %>% 
      subset(select = -c(user_id,product_id,
                         reordered,eval_set
                         #,order_dow
                         #,order_hour_of_day
      ))
    dtrain = xgb.DMatrix(data = as.matrix(subtrain.x))
    pred.xgb = predict(model,dtrain)
    
    return(pred.xgb)
  }else if(model.name=="glmnet"){
    
    x = data3 %>% 
      filter(eval_set==eval.name) %>%
      data.table %>% 
      subset(select = -c(user_id,product_id,
                         reordered,eval_set
                         #,order_dow
                         #,order_hour_of_day
      ))
    pred.glmnet<-
      predict(glmnet.model, s=0, as.matrix(x), type="response")
    
    return(pred.glmnet)
 
  }
}

ensemble.pred.fun = 
  function(pred.xgb,pred.glmnet,
           weight.xgb=0.5,weight.glmnet = 0.5){
    
    value = pred.xgb*weight.xgb+pred.glmnet*weight.glmnet
    return(value)
  }


xgb.fun=function(dtrain,seed=100){
  
  xgb_params=list( 	
    objective="reg:linear",
    booster = "gbtree",
    eta= 0.1, 
    max_depth= 7, 
    colsample_bytree= 0.7,
    subsample = 0.7
  )
  best_nrounds = 50#xgb_cv$best_iteration
  set.seed(seed)
  clf <- xgb.train(params=xgb_params,
                   data=dtrain, 
                   nrounds =best_nrounds,
                   watchlist = list(train = dtrain),
                   eval_metric='rmse'
  )
  return(clf)
}

xgb.fun2=function(dtrain,seed=100){
  
  xgb_params=list( 	
    objective="reg:linear",
    booster = "gbtree",
    eta= 0.1, 
    max_depth= 7, 
    colsample_bytree= 0.7,
    subsample = 0.7
  )
  best_nrounds = 60#xgb_cv$best_iteration
  set.seed(seed)
  clf <- xgb.train(params=xgb_params,
                   data=dtrain, 
                   nrounds =best_nrounds,
                   watchlist = list(train = dtrain),
                   eval_metric='rmse'
  )
  return(list(clf))
}


compare.train.fscore = 
  function(pred.train=pred.xgb,per=0.2,y = target){

  #per = 0.2
  pred.train[pred.train>per]=1
  pred.train[pred.train<per]=0
  
  train.table = table(pred.train,y)
  print(train.table)

  train.f.score = f_score(train.table)
  return(train.f.score)  
}

work.feature.fun=function(clf,var.name,feature_amount=8){
  
  #var.name = colnames(subtrain.x)
  #ncol(subtrain.x)
  importance <- xgb.importance(
    feature_names =  var.name, 
    model = clf )
  
  print(feature_amount)
  print(importance)
  feature = importance$Feature[1:feature_amount]
  
  return(feature)  
}


final.pred.fun=
  function(per,
           xgb.model,glmnet.model,
           data3,glmnet.data,
           weight.xgb=0.5,weight.glmnet = 0.5){ # data=data2

  final.pred.xgb = pred.fun( model = xgb.model, 
                             data3, 
                             model.name = "xgb",eval.name = "test")
  final.pred.glmnet = pred.fun( model = glmnet.model, 
                                glmnet.data, 
                                model.name = "glmnet",eval.name = "test")
  
  pred.test = 
    ensemble.pred.fun(final.pred.xgb,final.pred.glmnet,
                      weight.xgb,weight.glmnet) %>%
    as.numeric
  
  pred.test[pred.test>per]=1
  pred.test[pred.test<per]=0
  
  temp = data3 %>% 
    filter(eval_set=="test" ) %>% 
    data.table %>% 
    subset(select=c(user_id,product_id,reordered))
  
  temp$reordered = pred.test
  temp = temp[reordered==1]
  temp$reordered=NULL
  
  temp = temp[,.(products = list(product_id)),by=.(user_id)]
  
  temp2 = test %>% 
    subset(select=c(user_id,order_id)) %>%
    merge(temp,.,all=T,by="user_id")
  
  value = temp2 %>% subset(select=c(order_id,products))

  temp2 = mclapply(c(1:nrow(value)),function(i){#i=57
    value2 = Reduce( function(x,y) paste(x,y,sep=" "),
                    as.list( value$products[[i]]  )
    )

    if(length(value2)==0)   value2 = "None"
    value3 = data.table(order_id = value$order_id[i],products=value2)
    return(value3)
    
  },mc.cores=8, mc.preschedule = T)

  final.pred = do.call(rbind,temp2)

  return(final.pred)
}


feature.selection = function(data2,xgb.model,n=20){
  
  var.name = data2 %>% 
    filter(eval_set=="train") %>% 
    subset(select = -c(user_id,product_id,reordered,eval_set)) %>%
    colnames
  
  length(var.name)
  
  my.importance = var.name %>%
    xgb.importance(., model = xgb.model)
  
  print( my.importance )
  feature = my.importance$Feature[1:n]
  
  return(feature)
}

work.importance.fun = function(data3,xgb.model){
  var.name = data3[1,] %>% 
    subset(select = -c(user_id,product_id,reordered,eval_set)) %>%
    colnames
  
  my.importance = var.name %>%
    xgb.importance(., model = xgb.model)
  
  return( my.importance )
}






















