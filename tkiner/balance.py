import datetime
import os
import pandas as pd
import pymysql as py

def read_sql(cur, sql_order):
    try:
        cur.execute(sql_order)
        col = cur.description
        data = cur.fetchall()
        col_list = []
        for n in range(len(col)):
            col_list.append(col[n][0])
        frame = pd.DataFrame(list(data),columns=col_list)
    except:
        frame = pd.DataFrame()
        cur.close()
    return frame

class balance():
	
	def __init__(self, day,end):
		self.day = day
		self.end = end
		self.sql = '''SELECT
		t8.telphone,
	t8.enterprieName,
	t8.`name`,
	
	t8.`实际期初`,
	t8.结算金额,
	t8.系统提现金额,
	t8.提现失败返回,
	t8.`实际期末`,
	(t8.`实际期初` + t8.结算金额 - t8.系统提现金额 + t8.提现失败返回 - t8.`实际期末`) 差额
FROM
(
SELECT
	t7.enterprieName,
	t7.`name`,
	t7.telphone,
	t7.userId,
	ifnull(t7.`实际期末`,0) 实际期末,
	ifnull(t7.`实际期初`,0) 实际期初,
	ifnull(t7.结算金额,0) 结算金额,
	ifnull(t7.提现失败返回,0) 提现失败返回,
	ifnull(sum(f.drawMoney),0) as 系统提现金额
FROM
	(
	SELECT
		t6.enterprieName,
		t6.`name`,
		t6.telphone,
		t6.userId,
		t6.`实际期末`,
		t6.`实际期初`,
		t6.结算金额,
		t6.提现失败返回
	FROM
		(
		SELECT
			t5.enterprieName,
			t5.`name`,
			t5.telphone,
			t5.userId,
			t5.`实际期末`,
			t5.`期初` AS 实际期初,
			t5.结算金额,
			sum( e.drawMoney ) AS 提现失败返回
		FROM
			(
			SELECT
				t4.enterprieName,
				t4.`name`,
				t4.telphone,
				t4.userId,
				t4.`实际期末`,
				t4.`期初`,
				SUM( d.settleMoney ) AS 结算金额
			FROM
				(
				SELECT
					t3.telphone,
					t3.userId,
					t3.enterprieName,
					c.NAME,
					t3.`期初`,
					t3.`实际期末`
				FROM
					(
					SELECT
						t1.userId,
						t2.enterprieName,
						t1.createTime,
						SUM( t1.past ) AS 期初,
						SUM( t1.today ) + sum( t1.past ) AS 实际期末,
						cityId,
						t2.telphone
					FROM
						(
						SELECT
							vul_user_balance_record.userId,
-- 							( CASE WHEN date_format( createTime, '%Y-%m-%d' ) = date_format( NOW( ), '%Y-%m-%d' ) THEN score ELSE 0 END ) AS today,
-- 							( CASE WHEN date_format( createTime, '%Y-%m-%d' ) < date_format( NOW( ), '%Y-%m-%d' ) THEN score ELSE 0 END ) AS past,
							( CASE WHEN date_format( createTime, '%Y-%m-%d' ) =  '{0}' THEN score ELSE 0 END ) AS today,
							( CASE WHEN date_format( createTime, '%Y-%m-%d' ) < '{0}' THEN score ELSE 0 END ) AS past,
							createTime,
							score
						FROM
							vul_user_balance_record
						WHERE
							vul_user_balance_record.userType = '2'
						) AS t1
						INNER JOIN vul_wholesaler AS t2 ON t1.userId = t2.id
						AND t2.enterprieName NOT LIKE '%测试%'
						AND t2.enterprieName NOT IN ( '重庆尚融食品有限公司', '重庆幼鲜通食材有限公司', '重庆集鲜丰' )
					GROUP BY
						t2.enterprieName
					) AS t3
					INNER JOIN vul_location AS c ON t3.cityId = c.id
				) AS t4
				LEFT JOIN vul_order AS d ON d.sellerId = t4.userId
-- 				AND date_format( d.settleTime, '%Y-%m-%d' ) = date_format( NOW( ), '%Y-%m-%d' )
AND date_format( d.settleTime, '%Y-%m-%d' ) between '{0}' and '{1}'
			GROUP BY
				t4.enterprieName
			) AS t5
			LEFT JOIN vul_balance_drawcash AS e ON t5.userId = e.userId
-- 			AND date_format( e.updateTime, '%Y-%m-%d' ) = date_format( NOW( ), '%Y-%m-%d' )
			AND date_format( e.updateTime, '%Y-%m-%d' ) between '{0}' and '{1}'
			AND e.STATUS = 2
		GROUP BY
			t5.enterprieName
		) AS t6
	) AS t7
	LEFT JOIN vul_balance_drawcash AS f ON t7.userId = f.userId
-- 	AND date_format( f.createTime, '%Y-%m-%d' ) = date_format( NOW( ), '%Y-%m-%d' )
AND date_format( f.createTime, '%Y-%m-%d' ) between '{0}' and '{1}'
	GROUP BY
	t7.enterprieName
	) as t8
		'''.format(day, end)
		self.draw_Sql = '''
		SELECT
	t1.供应商名称,
	t1.申请提现日期,
	t1.提现成功日期,
	t1.申请提现金额,
	( CASE t1.STATUS WHEN 1 THEN '提现成功' WHEN 0 THEN '提现中' WHEN 2 THEN '提现失败' END ) AS 提现状态,
	t1.提现周期,
	vul_location.`name` AS 区域
FROM
	(
SELECT DISTINCT
	b.enterprieName 供应商名称,
	a.createTime 申请提现日期,
	a.treasurerCheckTime 提现成功日期,
	a.drawMoney 申请提现金额,
	a.STATUS,
	ROUND( TIMESTAMPDIFF( HOUR, a.createTime, a.treasurerCheckTime ) / 24, 2 ) AS 提现周期,
	b.cityId
FROM
	vul_balance_drawcash a,
	vul_wholesaler b
WHERE
	a.userId = b.id
	AND date_format( a.createTime, '%Y-%m-%d' ) >= '2018-07-31' -- and a.auditState='4'
-- and a.status='1'
	
	AND enterprieName NOT LIKE '%测试%'
	AND enterprieName NOT IN ( '重庆尚融食品有限公司', '重庆幼鲜通食材有限公司', '重庆集鲜丰' )
	) AS t1
INNER JOIN vul_location ON t1.cityId = vul_location.id'''
		self.db_vulacan = py.connect(host='101.132.176.215', user='tuxue', password='tuxue_123', port=23306,
		                             db='jxf_product', charset='utf8')
		self.cur_vulacan = self.db_vulacan.cursor()
		# self.day = datetime.datetime.now().strftime("%Y-%m-%d")
		self.path = os.getcwd()
		
		
		
	def balance_excel(self):
		df_balance = read_sql(self.cur_vulacan, self.sql)
		df_balance.to_excel('{0}/{1}供应商余额数据.xlsx'.format(self.path, self.day))
		a = print("{0}日供应商余额数据下载完成".format(self.day))
		return df_balance, a
		
	def draw_excel(self):
		df_balance = read_sql(self.cur_vulacan, self.draw_Sql)
		df_balance.to_excel('{0}/{1}提现记录数据.xlsx'.format(self.path, self.day))
		a = print("{0}提现记录数据".format(self.day))
		return df_balance, a
		
	def compare(self):
		df = self.balance_excel()
		settlement_amount = df["结算金额"].sum()
		Opening_amount = df['实际期初'].sum()
		draw_deposit = df["系统提现金额"].sum()
		return_amount = df["提现失败返回"].sum()
		print('期初金额总计：{0}'.format(Opening_amount))
		print('结算金额总计：{0}'.format(settlement_amount))
		print('提现金额总计：{0}'.format(draw_deposit))
		print('提现失败返回：{0}'.format(return_amount))
		if df[df["差额"] > 0 ].empty is True:
			print("今日数据无误")
		else:
			print("存在误差，数据如下")
			dfx = df[df['差额'] != 0]
			print(dfx)
			
			
if __name__ == "__main__":
	print("供应商余额请输入：1")
	print("提现记录请输入：2")
	number = int(input("请输入数字："))
	if number == 1:
		day = input("请输入起始日期：")
		end = input("请输入终止日期：")
		data = balance(day, end)
		data.compare()
	elif number == 2:
		day = input("请输入起始日期：")
		end = input("请输入终止日期：")
		data = balance(day, end)
		data.draw_excel()
	else:
		print("输入错误")
