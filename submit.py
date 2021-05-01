import json
import os
from contextlib import contextmanager
from typing import Dict
from typing import List
from canvasapi import Canvas
from canvasapi.quiz import QuizSubmission
from canvasapi.quiz import QuizSubmissionQuestion
from environs import Env
from git import Repo


env = Env()
env.read_env()
repo = Repo(".")
course_name = "Advanced Python for Data Science"
canvas = Canvas(env("CANVAS_URL"), env("CANVAS_TOKEN"))
courses = canvas.get_courses()
course = [c for c in courses if c.name == course_name][0]
all_quizzes = course.get_quizzes()
all_assignments = course.get_assignments()
masquerade = {}


def get_course_id(uuid=False):
    """return canvas course id"""
    if uuid:
        return course.uuid
    else:
        canvas_course_id = course.id
        return canvas_course_id


def get_quiz_id(quiz_name="Test Quiz"):
    """return quiz id"""
    quiz_list = list()
    quiz_id_list = list()
    for class_quiz in all_quizzes:
        quiz_list.append(class_quiz.title)
        quiz_id_list.append(class_quiz.id)
    if quiz_name in quiz_list:
        return quiz_id_list[quiz_list.index(quiz_name)]
    else:
        raise NameError("Desired quiz name not found")


def get_assignment_id(assignment_name="Final Project"):
    """return assignment id"""
    assignment_list = list()
    assignment_id_list = list()
    for class_assignment in all_assignments:
        assignment_list.append(class_assignment.name)
        assignment_id_list.append(class_assignment.id)
    if assignment_name in assignment_list:
        return assignment_id_list[assignment_list.index(assignment_name)]
    else:
        raise NameError("Desired assignment name not found")


def repo_dirty_quiz(repo_dirty=repo.is_dirty()):
    if repo_dirty:
        return 5153
    else:
        return 4031


def get_questions():
    quiz = course.get_quiz(get_quiz_id(), **masquerade)
    qsubmission = quiz.create_submission(**masquerade)
    questions = qsubmission.get_submission_questions(**masquerade)
    qsubmission.complete(**masquerade)
    return questions


def get_answers(questions: List[QuizSubmissionQuestion]) -> List[Dict]:
    """Creates answers for Canvas quiz questions"""
    # Formulate your answers - see docs for QuizSubmission.answer_submission_questions below
    # It should be a list of dicts, one per q, each with an 'id' and 'answer' field
    # The format of the 'answer' field depends on the question type
    # You are responsible for collating questions with the functions to call - do not hard code
    return [
        {"id": questions[0].id, "answer":{'1': 1221, '2': 1232}},
        {"id": questions[1].id, "answer": 4825},
    ]


def get_submission_comments(repo: Repo, qsubmission: QuizSubmission) -> Dict:
    """Get some info about this submission"""
    return dict(
        hexsha=repo.head.commit.hexsha[:8],
        submitted_from=repo.remotes.origin.url,
        dt=repo.head.commit.committed_datetime.isoformat(),
        branch="master",  # repo.active_branch.name,
        is_dirty=repo.is_dirty(),
        quiz_submission_id=qsubmission.id,
        quiz_attempt=qsubmission.attempt,
        travis_url="https://travis-ci.com/github/nikhar1210/2021sp-final-project-nikhar1210_Public/",
        project_link="https://2021sp-final-project-nikhar1210-public.readthedocs.io/en/latest/index.html",
    )


@contextmanager
def quiz_submission(assignment_id, quiz_id, allow_dirty=False):
    """

    :param course_id: course_id for quiz submission
    :param assignment_id: assignment for quiz submission
    :param quiz_id: quiz_id for quiz submission
    :param allow_dirty: allow if repo is not committed
    :return: quiz submission
    """

    if repo.is_dirty() and not allow_dirty:
        raise RuntimeError(
            "Must submit from a clean working directory - commit your code and rerun"
        )

    # Load canvas objects
    assignment = course.get_assignment(assignment_id, **masquerade)
    quiz = course.get_quiz(quiz_id, **masquerade)

    # Begin submissions
    URL = "https://github.com/nikhar1210/{}/commit/{}".format(
        os.path.basename(repo.working_dir), repo.head.commit.hexsha
    )
    # you MUST push to the classroom org, even if CI/CD
    # runs elsewhere (you can push anytime before peer review begins)

    qsubmission = None
    try:
        # Attempt quiz submission first - only submit assignment if successful
        qsubmission = quiz.create_submission(**masquerade)
        questions = qsubmission.get_submission_questions(**masquerade)
        # Submit your answers
        answers = get_answers(questions)
        yield answers
        qsubmission.answer_submission_questions(quiz_questions=answers, **masquerade)

    finally:
        if qsubmission is not None:
            qsubmission.complete(**masquerade)

            # Only submit assignment if quiz finished successfully
            assignment.submit(
                dict(
                    submission_type="online_url",
                    url=URL,
                ),
                comment=dict(
                    text_comment=json.dumps(get_submission_comments(repo, qsubmission))
                ),
                **masquerade,
            )


if __name__ == "__main__":
    with quiz_submission(assignment_id=get_assignment_id(), quiz_id=get_quiz_id()) as a:
        print(a)
