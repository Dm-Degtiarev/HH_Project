--get_companies_and_vacancies_count(): получает список всех компаний и количество
SELECT
	  e.employer_name
	, count(*) AS vacancy_cnt
FROM vacancy v
LEFT JOIN employer e ON v.employer_rk = e.employer_rk
WHERE v.valid_to_dttm = '9999-12-31'
GROUP BY e.employer_name
ORDER BY count(*) DESC;


--get_all_vacancies(): получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
SELECT
	  e.employer_name
	, v.vacancy_name
	, v.vacancy_sal_from
	, v.vacancy_sal_to
	, c.currency_name
	, v.vacancy_url
FROM vacancy v
LEFT JOIN employer e ON v.employer_rk = e.employer_rk
LEFT JOIN currency c ON c.currency_rk = v.currency_rk
WHERE v.valid_to_dttm = '9999-12-31'
ORDER BY vacancy_sal_to, vacancy_sal_from, employer_name;


--get_avg_salary(): получает среднюю зарплату по вакансиям.
/* Логика следующая: если обе суммы не пустые - берется средняя между ними, если не NULL только одна сумма - берется она. Затем, исходя из полученных цифр
считается уже средняя сумма по всем вакасиям, при этом, если сумма указана в другой валюте - идет умножение на текущий курс рубля к ней.
Вакансии где не указаны обе суммы не берутся в счет. Расчет идет в сумме 'NET' - до вычета налога (учитывается флаг - vacancy_sal_gross_flg)
TODO: можно заморочиться и сделать налоговый справочник всех стран на HH. Сейчас на все страны идет вычет 13% налога (если ГРОС) + можно сделать 20% если доход за год будет больше 5 000 000р.
*/
SELECT
	round(avg(CASE
				WHEN v.vacancy_sal_gross_flg = 0 THEN (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate
				ELSE (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate * 0.87
			  END), 0) AS avg_sal
FROM vacancy v
LEFT JOIN currency c ON c.currency_rk = v.currency_rk
WHERE v.valid_to_dttm = '9999-12-31'
AND (vacancy_sal_from IS NOT NULL OR vacancy_sal_to IS NOT NULL);


--get_vacancies_with_higher_salary(): получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
/*Зарплата по вакансии выводится NET (на руки). Расчет среднего число между from и to аналогична алгоритму прошлого скрипта*/
SELECT
	  vacancy_name
	, vacancy_url
	, CAST(avg_sal AS INT) AS avg_rub_sal_net
FROM (
		SELECT
			  v.vacancy_name
			, v.vacancy_url
			, v.vacancy_sal_from
			, v.vacancy_sal_to
			, c.currency_rub_rate
			, round(
					CASE
						WHEN v.vacancy_sal_gross_flg = 0 THEN (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate
						ELSE  (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate * 0.87
					END, 0) AS avg_sal
			, round(avg(
					CASE
						WHEN v.vacancy_sal_gross_flg = 0 THEN (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate
						ELSE (COALESCE(v.vacancy_sal_from, v.vacancy_sal_to) + COALESCE(v.vacancy_sal_to, v.vacancy_sal_from)) / 2 * c.currency_rub_rate * 0.87
					END
			) OVER(), 0)AS avg_sal_all
		FROM vacancy v
		LEFT JOIN currency c ON c.currency_rk = v.currency_rk
		WHERE v.valid_to_dttm = '9999-12-31'
		AND (vacancy_sal_from IS NOT NULL OR vacancy_sal_to IS NOT NULL)
)a
WHERE avg_sal > avg_sal_all;


--get_vacancies_with_keyword(): получает список всех вакансий, в названии которых содержатся переданные в метод слова, например “python”.
SELECT
	  v.vacancy_name
	, v.vacancy_url
FROM vacancy v
WHERE upper(vacancy_name) LIKE upper('%PYTHON%')
AND v.valid_to_dttm = '9999-12-31';
