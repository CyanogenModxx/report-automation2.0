from app import app
from flask import render_template,flash,redirect,url_for,make_response
from app.forms import LoginForm
from flask_login import current_user, login_user
from app.models import User
from flask_login import logout_user
from flask import request
from werkzeug.urls import url_parse
from flask_login import login_required
import time
import pymysql
from collections import OrderedDict
from sqlalchemy import create_engine
import numpy as np 
import pandas as pd 
from app.controller import Data_monitoring
from app.controller_past import get_allsite_GMV,get_allsite_sales,get_allsite_orders,get_allsite_traffic_source,get_allsite_reg_rate,get_allsite_conversion_rate,get_allsite_acitivation,get_search_term,get_allsite_uns,Single_site_data



@app.route('/',methods = ['GET','POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('site_Data_to_front'))
        
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user is None or not user.check_password(form.password.data):
            flash('Invalid username or password')
            return redirect(url_for('login'))
        login_user(user,remember=form.remember_me.data)
        next_page = request.args.get('next')
        if not next_page or url_parse(next_page).netloc != '':
            next_page = url_for('index')
        return redirect(next_page)
    return render_template('login.html', title='Sign In', form=form) 

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))
@app.route('/index')
def index():
    posts = [
        {
            'author': '          ———— Dylan Thomas',
            'name':'Do not go gentle into that good night',
            'one': 'Do not go gentle into that good night,',
            'two':'Old age should burn and rave at close of day,',
            'three':'Rage, rage against the dying of the light.',
            'four':'Though wise men at their end know dark is right,',
            'five':'Because their words had forked no lightning',
            'six':'Do not go gentle into that good night.',
            'seven':'Good men, the last wave by, crying how bright,',
            'eight':'Their frail deeds might have danced in a green bay,',
            'nine':'Rage, rage against the dying of the light.',
            'ten':'Wild men who caught and sang the sun in flight,',
            'eleven':'And learn, too late, they grieved it on its way,',
            'twelve':'Do not go gentle into that good night.',
            'thirteen':'Grave men, near death, who see with blinding sight',
            'fourteen':'Blind eyes could blaze like meteors and be gay,',
            'fifteen':'Rage, rage against the dying of the light.',
            'sixteen':'And you, my father, there on the sad height,',
            'seventeen':'Curse, bless, me now with your fierce tears.,I pray.',
            'eighteen':'Do not go gentle into that good night,',
            'nineteen':'Rage, rage against the dying of the light.'
            
        }
        ]
    return render_template('index.html',title = 'Home Page',posts = posts)



@app.route('/report/history/',methods = ['GET','POST'] )
@login_required
def site_Data_to_front():
    sitename = request.args.get('sitename', 'allsite')
    
    if sitename =='allsite':
        es_GMV,gg_GMV,gj_GMV,mj_GMV,sd_GMV,bs_GMV,month_list = get_allsite_GMV()
        allsite_sales = get_allsite_sales()
        all_site_order,all_site_paid_order,paid_rate,order_month = get_allsite_orders()
        all_site_traffic_source_dataset = get_allsite_traffic_source()
        new_users,reg_users,reg_rate,reg_month = get_allsite_reg_rate()
        convert_month,visitor_list,allsite_order_list,allsite_paid_order_list,convert_rate_list = get_allsite_conversion_rate()
        allsite_users,allsite_active_users,allsite_active_rate,allsite_active_month = get_allsite_acitivation()
        # allsite_desktop,allsite_mobile,allsite_tablet = get_allsite_device()
        search_term,search_times,search_rate = get_search_term()
        allsite_users_byday,allsite_newUsers_byday,allsite_sessions_byday,allsite_date_byday = get_allsite_uns()
        

        return render_template('allsite.html',
        sitename = sitename,
        allsite_sales = allsite_sales,
        es_GMV = es_GMV,gg_GMV=gg_GMV,gj_GMV=gj_GMV,mj_GMV=mj_GMV,sd_GMV=sd_GMV,bs_GMV=bs_GMV,month_list=month_list,
        all_site_order = all_site_order,all_site_paid_order = all_site_paid_order,paid_rate = paid_rate,order_month = order_month,
        all_site_traffic_source_dataset = all_site_traffic_source_dataset,
        new_users = new_users,reg_users = reg_users,reg_rate = reg_rate,reg_month = reg_month,
        convert_month =convert_month,visitor_list = visitor_list,allsite_order_list = allsite_order_list,allsite_paid_order_list = allsite_paid_order_list,convert_rate_list = convert_rate_list,
        allsite_users = allsite_users,allsite_active_users=allsite_active_users,allsite_active_rate=allsite_active_rate,allsite_active_month = allsite_active_month,
        # allsite_desktop = allsite_desktop,allsite_mobile = allsite_mobile,allsite_tablet = allsite_tablet,
        search_term = search_term,search_times=search_times,search_rate = search_rate,
        allsite_users_byday = allsite_users_byday,allsite_newUsers_byday = allsite_newUsers_byday,allsite_sessions_byday = allsite_sessions_byday,allsite_date_byday = allsite_date_byday)
    
    else:
        site = Single_site_data(sitename)
        site_month_list,site_sales = site.get_sales()
        site_GMV = site.get_GMV()
        site_paid_order,site_order,site_paid_rate = site.get_order()
        site_source_dataset = site.get_traffic_source()
        site_new_Users,site_register_users,site_register_rate,site_register_month = site.get_register_rate()
        site_users,site_activate_users,site_activate_rate,site_activate_month = site.get_activation()
        site_rate_dataset,site_paid_order_dataset = site.get_paidorder_bydevice()
        site_convert_month,site_visitor_list,site_order_list,site_paid_order_list,site_convert_rate_list = site.get_conversion_rate()
        site_users_byday,site_newUsers_byday,site_sessions_byday,site_date_byday = site.get_uns()

        return render_template('singgle_site.html',
        sitename = sitename,
        site_month_list =site_month_list,site_sales = site_sales,
        site_GMV = site_GMV,
        site_paid_order = site_paid_order,site_order = site_order,site_paid_rate = site_paid_rate,
        site_source_dataset  = site_source_dataset ,
        site_new_Users = site_new_Users,site_register_users = site_register_users,site_register_rate = site_register_rate,site_register_month =site_register_month,
        site_users = site_users,site_activate_users = site_activate_users,site_activate_rate = site_activate_rate,site_activate_month = site_activate_month,
        site_rate_dataset =site_rate_dataset,site_paid_order_dataset = site_paid_order_dataset,
        site_convert_month = site_convert_month,site_visitor_list = site_visitor_list,site_order_list = site_order_list,site_paid_order_list = site_paid_order_list,site_convert_rate_list = site_convert_rate_list,
        site_users_byday = site_users_byday,site_newUsers_byday = site_newUsers_byday,site_sessions_byday = site_sessions_byday,site_date_byday = site_date_byday)





    
    
@app.route('/report/current-month/',methods = ['GET','POST'])
@login_required
def get_current_month_data():
    sitename = request.args.get('sitename', 'allsite')
    target = Data_monitoring(sitename)  
    user_avg,user_alert_value,user_date,user = target.get_users()
    register_avg,register_alert,sign_up_date,sign_up_user = target.sign_up_user()
    order_avg,order_alert,current_month_order,current_month = target.get_current_orders()
    paidorder_avg,paidorder_alert,current_month_paidorder ,current_month_paid = target.get_current_paidorders()
    paid_month,current_payrate = target.get_pay_rate()
    sales_avg,sales_alert,current_month_sales,current_sales_date  = target.get_current_sales()
    date,payment,all_payment0,all_payment1,all_payment2,all_payment3,all_payment4,paid_payment0,paid_payment1,paid_payment2,paid_payment3,paid_payment4 = target.get_payment_rate()
    columns,order_data,paidorder_data = target.get_payment_percentage()
    product,amount = target.get_rank()

    return render_template('current-month.html',sitename=sitename,
    user_avg = user_avg,user_alert_value=user_alert_value,user_date =user_date,user =user,
    register_avg = register_avg,register_alert = register_alert,sign_up_date = sign_up_date,sign_up_user =sign_up_user,
    order_avg = order_avg,order_alert = order_alert,current_month_order = current_month_order,current_month = current_month,
    paidorder_avg = paidorder_avg,paidorder_alert = paidorder_alert,current_month_paidorder=current_month_paidorder,current_month_paid= current_month_paid,
    paid_month= paid_month,current_payrate=current_payrate,
    sales_avg = sales_avg,sales_alert = sales_alert,current_month_sales= current_month_sales,current_sales_date = current_sales_date,
    date =date ,payment = payment,all_payment0= all_payment0,all_payment1 = all_payment1,all_payment2 = all_payment2,all_payment3 = all_payment3,all_payment4 = all_payment4,
    paid_payment0 = paid_payment0,paid_payment1 = paid_payment1,paid_payment2 = paid_payment2,paid_payment3 = paid_payment3,paid_payment4 = paid_payment4,
    columns = columns,order_data = order_data,paidorder_data = paidorder_data,
    product =product ,amount = amount )


